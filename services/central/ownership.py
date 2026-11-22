import time
from threading import RLock

from shared.schema.network import load_network

class BusOwnership:
    def __init__(self, network: dict):
        self.home = {bus["bus_id"]: bus["edge_id"] for bus in network["buses"]}
        self.region_of = {bus["bus_id"]: bus["region_id"] for bus in network["buses"]}
        self.region_edges = {region["region_id"]: [region["primary_edge"], region["backup_edge"]] for region in network["regions"]}
        self.current = dict(self.home)

        #use wall clock to keep epoch increasing across central server restarts without having to store them
        start = int(time.time() * 1000)
        self.epoch = {region_id: start for region_id in self.region_edges}
        self.lock = RLock()

    def owner(self, bus_id) -> str | None:
        with self.lock:
            return self.current.get(bus_id)

    def buses_of(self, edge_id) -> set[str]:
        with self.lock:
            return {bus_id for bus_id, owner in self.current.items() if owner == edge_id}

    #copy of every bus's current owner, used to see which buses moved during a reconcile
    def owners(self) -> dict[str, str]:
        with self.lock:
            return dict(self.current)

    def regions(self) -> list[str]:
        return sorted(self.region_edges)

    #redirect bus stat report to the edge server that should be owning it now
    def reconcile(self, edge_status: dict[str, str]) -> set[str]:
        def healthy(edge_id):
            return edge_status.get(edge_id) == "healthy"

        #only a confirmed failure moves buses away; "unknown" and "recovering" keep what they have, so staggered startups do not cause churn
        def failed(edge_id):
            return edge_status.get(edge_id) == "unavailable"

        changed = set()
        with self.lock:
            for bus_id in sorted(self.current):
                region_id = self.region_of[bus_id]
                home, current = self.home[bus_id], self.current[bus_id]
                if healthy(home):
                    target = home   #if the home edge server is healthy: nth change or switching back to its primary server after a crash
                elif not failed(current):
                    target = current #home is not healthy, but the current owner has not failed: keep it there (no churn)
                else:
                    #the current owner has failed: move to the least-loaded healthy edge in the region, or stay put if none found
                    available_servers = [edge for edge in self.region_edges[region_id] if healthy(edge)]
                    #select server with the least load currently,
                    target = min(available_servers, key=lambda edge: (len(self.buses_of(edge)), edge)) if available_servers else current
                if target != current:
                    self.current[bus_id] = target
                    changed.add(region_id)
            for region_id in changed:
                self.epoch[region_id]+=1
        return changed
    
    def assignment(self, region_id) -> dict: 
        with self.lock:
            if region_id not in self.region_edges:
                raise KeyError(f"Unknown region {region_id}")
            owners = {bus_id: self.current[bus_id] for bus_id in sorted(self.current) if self.region_of[bus_id] == region_id}
            return {"region_id": region_id, "epoch": self.epoch[region_id], "owners": owners}


#the one ownership table used by the whole central process (ingestion fencing, health, failover loop)
#a central restart starts again from the home assignment; the failover loop corrects it within a timeout
bus_ownership = BusOwnership(load_network())