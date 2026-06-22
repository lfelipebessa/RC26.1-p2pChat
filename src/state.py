"""AppState (Fase 7): visão de leitura para a CLI. Guarda a identidade do peer
e formata as consultas (/conn, /rtt, /peers) lendo os componentes vivos
(PeerServer, KeepAlive, PeerTable) — sem duplicar estado (decisão A1).
"""


class AppState:
    def __init__(self, my_peer_id, config):
        self.my_peer_id = my_peer_id
        self.config = config
        self.public_ip = None

    def render_conns(self, server, table):
        conns = server.connections
        if not conns:
            return "Nenhuma conexão ativa."
        linhas = ["Conexões ativas:"]
        for peer_id, conn in sorted(conns.items()):
            estado = table.peers[peer_id].state if peer_id in table.peers else conn.state
            linhas.append(f"  {peer_id:<24} {conn.direction:<9} {estado}")
        return "\n".join(linhas)

    def render_rtt(self, keep_alive, server):
        conns = server.connections
        if not conns:
            return "Sem conexões para medir RTT."
        linhas = ["RTT por peer:"]
        for peer_id in sorted(conns):
            avg = keep_alive.average_rtt(peer_id)
            valor = ("%.1f ms" % avg) if avg is not None else "—"
            linhas.append(f"  {peer_id:<24} {valor}")
        overall = keep_alive.overall_average()
        if overall is not None:
            linhas.append(f"  média geral: {overall:.1f} ms")
        return "\n".join(linhas)

    def render_peers(self, table):
        if not table.peers:
            return "Nenhum peer conhecido."
        linhas = ["Peers conhecidos:"]
        for peer_id, info in sorted(table.peers.items()):
            linhas.append(f"  {peer_id:<24} {info.state:<11} {info.ip}:{info.port}")
        return "\n".join(linhas)
