import express from "express";
import http from "http";
import { Server, Socket } from "socket.io";
import cors from "cors";

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
  },
});

// Signaling payloads are opaque to the server (relayed as-is).
// Defined locally so we don't need DOM lib types in a Node project.
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

// Queue of socket IDs waiting to be matched
let waitingQueue: string[] = [];

// Maps each socket.id to their partner's socket.id
const pairs: Record<string, string> = {};

// Helper: break a pair and notify the partner
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

  // --- MATCHMAKING ---
  socket.on("find-match", () => {
    // Remove from queue if already in it (safety check)
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);

    if (waitingQueue.length > 0) {
      const partnerId = waitingQueue.shift()!;
      const partner = io.sockets.sockets.get(partnerId);

      if (!partner) {
        // That waiting user disconnected, add current user to queue
        waitingQueue.push(socket.id);
        socket.emit("waiting");
        return;
      }

      // Record the pairing both ways
      pairs[socket.id] = partnerId;
      pairs[partnerId] = socket.id;

      // The waiting user (partner) creates the WebRTC offer
      partner.emit("matched", { initiator: true });
      socket.emit("matched", { initiator: false });

      console.log(`Paired: ${partnerId} <-> ${socket.id}`);
    } else {
      // No one waiting — join the queue
      waitingQueue.push(socket.id);
      socket.emit("waiting");
      console.log("Waiting:", socket.id);
    }
  });

  // --- WEBRTC SIGNALING ---
  // Just relay messages between paired users — server doesn't interpret these

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

  // --- TEXT CHAT ---
  socket.on("chat-message", (text: unknown) => {
    if (typeof text !== "string") return;
    const trimmed = text.trim().slice(0, 500);
    if (!trimmed) return;

    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("chat-message", { text: trimmed });
    }
  });

  // --- REACTIONS ---
  const ALLOWED_REACTIONS = new Set(["👋", "🔥", "😂", "👏", "❤️"]);
  socket.on("reaction", (emoji: unknown) => {
    if (typeof emoji !== "string" || !ALLOWED_REACTIONS.has(emoji)) return;

    const partnerId = pairs[socket.id];
    if (partnerId) {
      io.to(partnerId).emit("reaction", { emoji });
    }
  });

  // --- REPORT ---
  socket.on("report", (payload: { reason?: string }) => {
    const partnerId = pairs[socket.id];
    console.log("Report:", {
      from: socket.id,
      about: partnerId ?? null,
      reason: payload?.reason ?? "unspecified",
    });
    socket.emit("report-received");
  });

  // --- NEXT / SKIP ---
  socket.on("next", () => {
    disconnectFromPartner(socket);
    // Re-queue this user (dedupe in case they were already waiting)
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);
    waitingQueue.push(socket.id);
    socket.emit("waiting");
  });

  // --- DISCONNECT ---
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);
    disconnectFromPartner(socket);
  });
});

const PORT = 5000;
server.listen(PORT, () => {
  console.log(`Signaling server running on http://localhost:${PORT}`);
});
