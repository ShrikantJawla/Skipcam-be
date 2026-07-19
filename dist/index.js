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
    // Non-browser clients / same-host probes may omit Origin
    if (!origin)
        return true;
    if (CLIENT_ORIGINS.includes(origin))
        return true;
    // Vercel preview deployments
    if (/^https:\/\/[a-z0-9-]+\.vercel\.app$/i.test(origin))
        return true;
    return false;
}
const corsOptions = {
    origin(origin, callback) {
        if (isAllowedOrigin(origin)) {
            callback(null, true);
        }
        else {
            callback(new Error(`CORS blocked for origin: ${origin}`));
        }
    },
    methods: ["GET", "POST"],
    credentials: true,
};
const app = (0, express_1.default)();
app.use((0, cors_1.default)(corsOptions));
// Queue of socket IDs waiting to be matched
let waitingQueue = [];
// Maps each socket.id to their partner's socket.id
const pairs = {};
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
const server = http_1.default.createServer(app);
const io = new socket_io_1.Server(server, {
    cors: corsOptions,
    transports: ["websocket", "polling"],
    allowEIO3: true,
});
function disconnectFromPartner(socket) {
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
io.on("connection", (socket) => {
    console.log("User connected:", socket.id);
    socket.on("find-match", () => {
        // Leave any existing pair before re-queueing
        disconnectFromPartner(socket);
        waitingQueue = waitingQueue.filter((id) => id !== socket.id);
        // Drop stale IDs that are no longer connected
        waitingQueue = waitingQueue.filter((id) => io.sockets.sockets.has(id));
        if (waitingQueue.length > 0) {
            const partnerId = waitingQueue.shift();
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
        }
        else {
            waitingQueue.push(socket.id);
            socket.emit("waiting");
            console.log("Waiting:", socket.id, "| queue:", waitingQueue.length);
        }
    });
    socket.on("offer", (data) => {
        const partnerId = pairs[socket.id];
        if (partnerId) {
            io.to(partnerId).emit("offer", data);
        }
    });
    socket.on("answer", (data) => {
        const partnerId = pairs[socket.id];
        if (partnerId) {
            io.to(partnerId).emit("answer", data);
        }
    });
    socket.on("ice-candidate", (data) => {
        const partnerId = pairs[socket.id];
        if (partnerId) {
            io.to(partnerId).emit("ice-candidate", data);
        }
    });
    socket.on("chat-message", (text) => {
        if (typeof text !== "string")
            return;
        const trimmed = text.trim().slice(0, 500);
        if (!trimmed)
            return;
        const partnerId = pairs[socket.id];
        if (partnerId) {
            io.to(partnerId).emit("chat-message", { text: trimmed });
        }
    });
    const ALLOWED_REACTIONS = new Set(["👋", "🔥", "😂", "👏", "❤️"]);
    socket.on("reaction", (emoji) => {
        if (typeof emoji !== "string" || !ALLOWED_REACTIONS.has(emoji))
            return;
        const partnerId = pairs[socket.id];
        if (partnerId) {
            io.to(partnerId).emit("reaction", { emoji });
        }
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
