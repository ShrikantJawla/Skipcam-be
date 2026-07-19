"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const http_1 = __importDefault(require("http"));
const socket_io_1 = require("socket.io");
const cors_1 = __importDefault(require("cors"));
const CLIENT_ORIGINS = (process.env.CLIENT_ORIGINS ||
    "http://localhost:3000,https://skipcam-fe.vercel.app")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);
function isAllowedOrigin(origin) {
    if (!origin)
        return true;
    if (CLIENT_ORIGINS.includes(origin))
        return true;
    if (/^https:\/\/[a-z0-9-]+\.vercel\.app$/i.test(origin))
        return true;
    return false;
}
const corsOptions = {
    origin(origin, callback) {
        if (isAllowedOrigin(origin))
            callback(null, true);
        else
            callback(new Error(`CORS blocked for origin: ${origin}`));
    },
    methods: ["GET", "POST"],
    credentials: true,
};
const app = (0, express_1.default)();
app.use((0, cors_1.default)(corsOptions));
let waitingQueue = [];
const pairs = {};
function pruneQueue() {
    waitingQueue = waitingQueue.filter((id) => io.sockets.sockets.has(id));
}
function prunePairs() {
    for (const id of Object.keys(pairs)) {
        const partnerId = pairs[id];
        if (!io.sockets.sockets.has(id) || !io.sockets.sockets.has(partnerId)) {
            delete pairs[id];
            delete pairs[partnerId];
        }
    }
}
function disconnectFromPartner(socket) {
    const partnerId = pairs[socket.id];
    if (partnerId) {
        const partner = io.sockets.sockets.get(partnerId);
        if (partner)
            partner.emit("partner-left");
        delete pairs[partnerId];
    }
    delete pairs[socket.id];
}
function tryMatch(socket) {
    pruneQueue();
    prunePairs();
    waitingQueue = waitingQueue.filter((id) => id !== socket.id);
    if (waitingQueue.length === 0) {
        waitingQueue.push(socket.id);
        socket.emit("waiting");
        console.log("Waiting:", socket.id, "| queue:", waitingQueue.length);
        return;
    }
    const partnerId = waitingQueue.shift();
    const partner = io.sockets.sockets.get(partnerId);
    if (!partner) {
        waitingQueue.push(socket.id);
        socket.emit("waiting");
        return;
    }
    pairs[socket.id] = partnerId;
    pairs[partnerId] = socket.id;
    // Partner was waiting longer → they create the offer
    partner.emit("matched", { initiator: true });
    socket.emit("matched", { initiator: false });
    console.log(`Paired: ${partnerId} <-> ${socket.id}`);
}
function relay(socket, event, data) {
    const partnerId = pairs[socket.id];
    if (!partnerId)
        return;
    if (!io.sockets.sockets.has(partnerId)) {
        disconnectFromPartner(socket);
        socket.emit("partner-left");
        return;
    }
    io.to(partnerId).emit(event, data);
}
app.get("/", (_req, res) => {
    res.type("text").send("Skipcam signaling server is running");
});
app.get("/health", (_req, res) => {
    pruneQueue();
    prunePairs();
    res.json({
        ok: true,
        waiting: waitingQueue.length,
        pairs: Object.keys(pairs).length / 2,
        clients: io.engine.clientsCount,
    });
});
/** ICE servers for clients (STUN always; TURN from env when configured) */
app.get("/ice", (_req, res) => {
    const iceServers = [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
    ];
    const turnUrls = process.env.TURN_URLS?.split(",").map((u) => u.trim()).filter(Boolean);
    if (turnUrls?.length && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
        iceServers.push({
            urls: turnUrls,
            username: process.env.TURN_USERNAME,
            credential: process.env.TURN_CREDENTIAL,
        });
    }
    res.json({ iceServers });
});
const server = http_1.default.createServer(app);
const io = new socket_io_1.Server(server, {
    cors: corsOptions,
    transports: ["websocket", "polling"],
    allowEIO3: true,
    pingTimeout: 60000,
    pingInterval: 25000,
});
io.on("connection", (socket) => {
    console.log("User connected:", socket.id);
    socket.on("find-match", () => {
        disconnectFromPartner(socket);
        tryMatch(socket);
    });
    socket.on("offer", (data) => {
        relay(socket, "offer", data);
    });
    socket.on("answer", (data) => {
        relay(socket, "answer", data);
    });
    socket.on("ice-candidate", (data) => {
        relay(socket, "ice-candidate", data);
    });
    socket.on("chat-message", (text) => {
        if (typeof text !== "string")
            return;
        const trimmed = text.trim().slice(0, 500);
        if (!trimmed)
            return;
        relay(socket, "chat-message", { text: trimmed });
    });
    const ALLOWED_REACTIONS = new Set(["👋", "🔥", "😂", "👏", "❤️"]);
    socket.on("reaction", (emoji) => {
        if (typeof emoji !== "string" || !ALLOWED_REACTIONS.has(emoji))
            return;
        relay(socket, "reaction", { emoji });
    });
    socket.on("report", (payload) => {
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
        tryMatch(socket);
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
