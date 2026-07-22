import express from "express";
import http from "http";
import { Server, Socket } from "socket.io";
import cors from "cors";

const CLIENT_ORIGINS = (
  process.env.CLIENT_ORIGINS ||
  "http://localhost:3000,https://skipcam-fe.vercel.app"
)
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

function isAllowedOrigin(origin: string | undefined): boolean {
  // Non-browser clients / same-host probes may omit Origin
  if (!origin) return true;
  if (CLIENT_ORIGINS.includes(origin)) return true;
  // Vercel preview deployments
  if (/^https:\/\/[a-z0-9-]+\.vercel\.app$/i.test(origin)) return true;
  // Local tunneling for device testing (localtunnel / ngrok / Cloudflare)
  if (/^https:\/\/[a-z0-9-]+\.loca\.lt$/i.test(origin)) return true;
  if (/^https:\/\/[a-z0-9-]+\.trycloudflare\.com$/i.test(origin)) return true;
  if (
    /^https:\/\/[a-z0-9-]+\.(ngrok-free\.app|ngrok-free\.dev|ngrok\.app)$/i.test(
      origin,
    )
  ) {
    return true;
  }
  return false;
}

const corsOptions: cors.CorsOptions = {
  origin(origin, callback) {
    if (isAllowedOrigin(origin)) {
      callback(null, true);
    } else {
      callback(new Error(`CORS blocked for origin: ${origin}`));
    }
  },
  methods: ["GET", "POST"],
  credentials: true,
};

const app = express();
app.use(cors(corsOptions));

// Queue of socket IDs waiting to be matched
let waitingQueue: string[] = [];

// Maps each socket.id to their partner's socket.id
const pairs: Record<string, string> = {};

app.get("/", (_req, res) => {
  res.type("text").send("Skipcam signaling server is running");
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    waiting: waitingQueue.length,
    pairs: Object.keys(pairs).length / 2,
  });
});

const server = http.createServer(app);

const io = new Server(server, {
  cors: corsOptions,
  transports: ["websocket", "polling"],
  allowEIO3: true,
});

// Signaling payloads are opaque to the server (relayed as-is).
type SessionDescriptionInit = {
  type?: "offer" | "answer" | "pranswer" | "rollback";
  sdp?: string;
};

type IceCandidateInit = {
  candidate?: string;
  sdpMid?: string | null;
  sdpMLineIndex?: number | null;
  usernameFragment?: string | null;
};

function disconnectFromPartner(socket: Socket): void {
  const partnerId = pairs[socket.id];
  if (partnerId) {
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) {
      partner.emit("partner-left");
    }
    delete pairs[partnerId];
  }
  delete pairs[socket.id];
}

io.on("connection", (socket: Socket) => {
  console.log("User connected:", socket.id);

  socket.on("find-match", () => {
    // Leave any existing pair before re-queueing
    disconnectFromPartner(socket);
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);

    // Drop stale IDs that are no longer connected
    waitingQueue = waitingQueue.filter((id) => io.sockets.sockets.has(id));

    if (waitingQueue.length > 0) {
      const partnerId = waitingQueue.shift()!;
      const partner = io.sockets.sockets.get(partnerId);

      if (!partner) {
        waitingQueue.push(socket.id);
        socket.emit("waiting");
        return;
      }

      pairs[socket.id] = partnerId;
      pairs[partnerId] = socket.id;

      partner.emit("matched", { initiator: true });
      socket.emit("matched", { initiator: false });

      console.log(`Paired: ${partnerId} <-> ${socket.id}`);
    } else {
      waitingQueue.push(socket.id);
      socket.emit("waiting");
      console.log("Waiting:", socket.id, "| queue:", waitingQueue.length);
    }
  });

  socket.on("offer", (data: SessionDescriptionInit) => {
    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("offer", data);
    }
  });

  socket.on("answer", (data: SessionDescriptionInit) => {
    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("answer", data);
    }
  });

  socket.on("ice-candidate", (data: IceCandidateInit) => {
    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("ice-candidate", data);
    }
  });

  socket.on("chat-message", (text: unknown) => {
    if (typeof text !== "string") return;
    const trimmed = text.trim().slice(0, 500);
    if (!trimmed) return;

    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("chat-message", { text: trimmed });
    }
  });

  const ALLOWED_REACTIONS = new Set(["👋", "🔥", "😂", "👏", "❤️"]);
  socket.on("reaction", (emoji: unknown) => {
    if (typeof emoji !== "string" || !ALLOWED_REACTIONS.has(emoji)) return;

    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("reaction", { emoji });
    }
  });

  socket.on("report", (payload: { reason?: string }) => {
    const partnerId = pairs[socket.id];
    console.log("Report:", {
      from: socket.id,
      about: partnerId ?? null,
      reason: payload?.reason ?? "unspecified",
    });
    socket.emit("report-received");
  });

  socket.on("next", () => {
    disconnectFromPartner(socket);
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);
    waitingQueue.push(socket.id);
    socket.emit("waiting");
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);
    disconnectFromPartner(socket);
  });
});

const PORT = Number(process.env.PORT) || 5000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Signaling server running on port ${PORT}`);
  console.log(`Allowed origins: ${CLIENT_ORIGINS.join(", ")} (+ *.vercel.app)`);
});
