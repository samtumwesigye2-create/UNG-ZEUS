"""UNG-ZEUS robust storage-fabric primitives.
Production adapters (VAULT/JANUS/S3/EC) implement the contracts defined here.
"""
import hashlib, heapq, itertools, json, threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

class Tier(str, Enum): CACHE='cache'; HOT='hot'; WARM='warm'; COLD='cold'; ARCHIVE='archive'
class Classification(str, Enum): PUBLIC='public'; INTERNAL='internal'; CONFIDENTIAL='confidential'; RESTRICTED='restricted'
@dataclass(frozen=True)
class FailureDomain: region:str; facility:str; rack:str; node:str
@dataclass
class StorageNode:
    node_id:str; domain:FailureDomain; capacity_bytes:int; used_bytes:int=0; healthy:bool=True; latency_ms:float=0; tiers:list=field(default_factory=lambda:[Tier.HOT])
    @property
    def free_bytes(self): return max(0,self.capacity_bytes-self.used_bytes)
@dataclass
class PlacementPolicy:
    replicas:int=3; min_regions:int=2; erasure_data_shards:int=0; erasure_parity_shards:int=0; worm:bool=False; retention_days:int=365; priority:int=50

class PlacementError(RuntimeError): pass
class PlacementBrain:
    def choose(self,nodes,size,policy,tier=Tier.HOT):
        candidates=sorted([n for n in nodes if n.healthy and tier in n.tiers and n.free_bytes>=size],key=lambda n:(n.latency_ms,-n.free_bytes))
        chosen=[]; regions=set(); facilities=set()
        for n in candidates:
            if len(chosen)>=policy.replicas: break
            if n.domain.region not in regions or n.domain.facility not in facilities:
                chosen.append(n); regions.add(n.domain.region); facilities.add(n.domain.facility)
        for n in candidates:
            if len(chosen)>=policy.replicas: break
            if n not in chosen: chosen.append(n); regions.add(n.domain.region)
        if len(chosen)<policy.replicas or len(regions)<min(policy.min_regions,policy.replicas): raise PlacementError('insufficient healthy failure domains')
        return chosen

def policy_for(classification,critical=False):
    c=Classification(classification)
    if critical:return PlacementPolicy(3,2,worm=True,retention_days=3650,priority=100)
    if c==Classification.RESTRICTED:return PlacementPolicy(4,2,worm=True,retention_days=2555,priority=90)
    if c==Classification.CONFIDENTIAL:return PlacementPolicy(3,2,retention_days=1825,priority=75)
    return PlacementPolicy()

def desired_tier(age_days,accesses_30d,pinned=False):
    if pinned:return Tier.HOT
    if accesses_30d>=20:return Tier.CACHE
    if age_days<30:return Tier.HOT
    if age_days<90:return Tier.WARM
    if age_days<365:return Tier.COLD
    return Tier.ARCHIVE

def chunks(data,size=8*1024*1024):
    for offset in range(0,len(data),size):
        part=data[offset:offset+size]; yield offset//size,part,hashlib.sha256(part).hexdigest()

class PriorityBackpressureQueue:
    def __init__(self,max_items=10000): self.max_items=max_items; self.q=[]; self.seq=itertools.count(); self.lock=threading.Lock()
    def put(self,item,priority=50):
        with self.lock:
            if len(self.q)>=self.max_items: raise OverflowError('ZEUS backpressure active')
            heapq.heappush(self.q,(-priority,next(self.seq),item))
    def get(self):
        with self.lock:return heapq.heappop(self.q)[2] if self.q else None

class HashChainAudit:
    def __init__(self): self.records=[]
    def append(self,actor,action,resource,details=None):
        prev=self.records[-1]['hash'] if self.records else '0'*64
        body={'at':datetime.now(timezone.utc).isoformat(),'actor':actor,'action':action,'resource':resource,'details':details or {},'prev_hash':prev}
        body['hash']=hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest(); self.records.append(body); return body

class NamespaceRegistry:
    def __init__(self): self.items={n:{'quota_bytes':None,'used_bytes':0} for n in ('HORUS','UGAMAP','VECTOR','NEMSIS','NOVA','APOLLO','PULSAR','NEXUS')}
    def register(self,name,quota_bytes=None): self.items[name.upper()]={'quota_bytes':quota_bytes,'used_bytes':0}
    def has_capacity(self,name,size):
        n=self.items[name.upper()]; return n['quota_bytes'] is None or n['used_bytes']+size<=n['quota_bytes']

class DedupIndex:
    def __init__(self): self.by_hash={}
    def lookup(self,digest): return self.by_hash.get(digest)
    def record(self,digest,ref): self.by_hash.setdefault(digest,ref); return self.by_hash[digest]

class SnapshotCatalog:
    def __init__(self): self.snapshots={}
    def create(self,name,manifest):
        self.snapshots[name]={'created_at':datetime.now(timezone.utc).isoformat(),'manifest':dict(manifest)}; return self.snapshots[name]
    def restore_manifest(self,name): return dict(self.snapshots[name]['manifest'])

class RepairPlanner:
    def plan(self,object_id,replicas,desired):
        healthy=[r for r in replicas if r.get('healthy') and r.get('checksum_ok')]
        return {'object_id':object_id,'healthy_replicas':len(healthy),'repairs_needed':max(0,desired-len(healthy)),'source':healthy[0] if healthy else None}

def capacity_metrics(nodes):
    total=sum(n.capacity_bytes for n in nodes); used=sum(n.used_bytes for n in nodes)
    return {'nodes':len(nodes),'healthy_nodes':sum(n.healthy for n in nodes),'capacity_bytes':total,'used_bytes':used,'free_bytes':max(0,total-used),'utilization':used/total if total else 0}
def forecast_days_to_full(capacity,used,daily_growth): return None if daily_growth<=0 else max(0,(capacity-used)/daily_growth)

class EnvelopeCryptoProvider:
    """Implemented by UNG-VAULT; ZEUS never owns master keys."""
    def encrypt_data_key(self,key,context): raise NotImplementedError
    def decrypt_data_key(self,wrapped,context): raise NotImplementedError
class JanusAuthorizer:
    @staticmethod
    def scope(action,namespace): return f'zeus:{action}:{namespace.lower()}'
    def authorize(self,claims,action,namespace): return self.scope(action,namespace) in set(claims.get('scopes',[]))
class ErasureCodingProvider:
    def encode(self,data,data_shards,parity_shards): raise NotImplementedError
    def decode(self,shards,data_shards,parity_shards): raise NotImplementedError
class S3GatewayContract:
    def put_object(self,bucket,key,body,metadata=None): raise NotImplementedError
    def get_object(self,bucket,key,byte_range=None): raise NotImplementedError
class EventSink:
    """NEXUS/PULSAR adapter contract for object.created, archived, repaired and integrity events."""
    def publish(self,event_type,payload): raise NotImplementedError
