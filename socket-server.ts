/**
 * EnganchaSerieLAT — Servidor WebSocket
 * Deploy: Railway  |  Puerto: process.env.PORT
 */
import { createServer, IncomingMessage, ServerResponse } from "http";
import { Server, type Socket } from "socket.io";

const PORT       = parseInt(process.env.PORT ?? "3001", 10);
const FRONTEND   = process.env.FRONTEND_URL ?? "https://enganchaserielat.vercel.app";
const INT_SECRET = process.env.INTERNAL_SECRET ?? "";

/* ── HTTP server base ── */
const httpServer = createServer((req: IncomingMessage, res: ServerResponse) => {
  /* Health check para Railway */
  if (req.method === "GET" && req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", service: "enganchaserielat-socket" }));
    return;
  }

  /* API interna: actualizar votos desde Next.js → broadcast a clientes */
  if (req.method === "POST" && req.url === "/internal/votos") {
    const auth = req.headers["x-internal-secret"];
    if (INT_SECRET && auth !== INT_SECRET) {
      res.writeHead(401); res.end(); return;
    }
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        const { votacionId, votos_a, votos_b } = JSON.parse(body);
        votos.set(votacionId, { a: votos_a, b: votos_b });
        io.to(`vot:${votacionId}`).emit("votos:update", {
          votacionId, votos_a, votos_b, total: votos_a + votos_b,
        });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } catch {
        res.writeHead(400); res.end("Bad request");
      }
    });
    return;
  }

  res.writeHead(404); res.end();
});

/* ── Socket.io ── */
const io = new Server(httpServer, {
  cors: {
    origin:      [FRONTEND, "http://localhost:3000"],
    methods:     ["GET", "POST"],
    credentials: true,
  },
  pingTimeout:  20000,
  pingInterval: 10000,
});

/* Contadores en memoria (reemplazar con Redis en alta escala) */
const viewers = new Map<string, Set<string>>();
const votos   = new Map<string, { a: number; b: number }>();

io.on("connection", (socket: Socket) => {
  console.log(`[+] ${socket.id} conectado (total: ${io.engine.clientsCount})`);

  /* ── Sala de episodio: viewers en tiempo real ── */
  socket.on("join:episodio", ({ episodioId }: { episodioId: string }) => {
    socket.join(`ep:${episodioId}`);
    if (!viewers.has(episodioId)) viewers.set(episodioId, new Set());
    viewers.get(episodioId)!.add(socket.id);
    io.to(`ep:${episodioId}`).emit("viewers:update", {
      episodioId, count: viewers.get(episodioId)!.size,
    });
  });

  socket.on("leave:episodio", ({ episodioId }: { episodioId: string }) => {
    socket.leave(`ep:${episodioId}`);
    viewers.get(episodioId)?.delete(socket.id);
    io.to(`ep:${episodioId}`).emit("viewers:update", {
      episodioId, count: viewers.get(episodioId)?.size ?? 0,
    });
  });

  /* ── Sala de votación: votos en tiempo real ── */
  socket.on("join:votacion", ({ votacionId }: { votacionId: string }) => {
    socket.join(`vot:${votacionId}`);
    const v = votos.get(votacionId) ?? { a: 0, b: 0 };
    socket.emit("votos:update", {
      votacionId, votos_a: v.a, votos_b: v.b, total: v.a + v.b,
    });
  });

  socket.on("leave:votacion", ({ votacionId }: { votacionId: string }) => {
    socket.leave(`vot:${votacionId}`);
  });

  /* ── Reacciones flotantes ── */
  socket.on("reaccion", ({ episodioId, emoji }: { episodioId: string; emoji: string }) => {
    socket.to(`ep:${episodioId}`).emit("reaccion:nueva", {
      emoji, x: Math.floor(Math.random() * 80) + 10,
    });
  });

  /* ── Desconexión ── */
  socket.on("disconnect", () => {
    viewers.forEach((set, episodioId) => {
      if (set.delete(socket.id)) {
        io.to(`ep:${episodioId}`).emit("viewers:update", {
          episodioId, count: set.size,
        });
      }
    });
    console.log(`[-] ${socket.id} desconectado (total: ${io.engine.clientsCount})`);
  });
});

httpServer.listen(PORT, () => {
  console.log(`✅ Socket.io escuchando en :${PORT}`);
  console.log(`   CORS permitido para: ${FRONTEND}`);
});
