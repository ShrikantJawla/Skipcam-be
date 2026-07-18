"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const http_1 = __importDefault(require("http"));
const socket_io_1 = require("socket.io");
const cors_1 = __importDefault(require("cors"));
const app = (0, express_1.default)();
app.use((0, cors_1.default)());
const server = http_1.default.createServer(app);
const io = new socket_io_1.Server(server, {
    cors: {
        origin: "http://localhost:3000", // Next.js dev server
        methods: ["GET", "POST"],
    },
});
// Queue of socket IDs waiting to be matched
let waitingQueue = [];
// Maps each socket.id to their partner's socket.id
const pairs = {};
// Helper: break a pair and notify the partner
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
    // --- MATCHMAKING ---
    socket.on("find-match", () => {
        // Remove from queue if already in it (safety check)
        waitingQueue = waitingQueue.filter((id) => id !== socket.id);
        if (waitingQueue.length > 0) {
            const partnerId = waitingQueue.shift();
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
        }
        else {
            // No one waiting — join the queue
            waitingQueue.push(socket.id);
            socket.emit("waiting");
            console.log("Waiting:", socket.id);
        }
    });
    // --- WEBRTC SIGNALING ---
    // Just relay messages between paired users — server doesn't interpret these
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
    // --- NEXT / SKIP ---
    socket.on("next", () => {
        disconnectFromPartner(socket);
        // Re-queue this user
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
const PORT = 4000;
server.listen(PORT, () => {
    console.log(`Signaling server running on http://localhost:${PORT}`);
});
