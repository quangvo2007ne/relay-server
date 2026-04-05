const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 10e6, // 10MB for H.264 segments
});

const rooms = new Map();

app.get("/health", (req, res) => {
  res.json({ status: "ok", rooms: rooms.size, uptime: process.uptime() });
});

io.on("connection", (socket) => {
  console.log(`[+] ${socket.id} connected`);

  // === ROOM MANAGEMENT ===
  socket.on("create-room", (roomId) => {
    socket.join(roomId);
    rooms.set(roomId, {
      host: socket.id,
      clients: new Set(),
      initSegment: null,   // Cache H.264 init segment
      createdAt: Date.now()
    });
    socket.emit("room-created", roomId);
    console.log(`[ROOM] Created: ${roomId}`);
  });

  socket.on("join-room", (roomId) => {
    const room = rooms.get(roomId);
    if (!room) {
      socket.emit("error-msg", "Room not found");
      return;
    }
    socket.join(roomId);
    room.clients.add(socket.id);
    socket.emit("room-joined", roomId);

    // Gửi cached init segment cho client mới (nếu có)
    if (room.initSegment) {
      socket.emit("stream-init", room.initSegment);
      console.log(`[ROOM] Sent cached init segment to ${socket.id}`);
    }

    io.to(room.host).emit("client-joined", socket.id);
    console.log(`[ROOM] ${socket.id} joined ${roomId}`);
  });

  // === H.264 STREAM: Init segment (ftyp + moov) ===
  socket.on("stream-init", (data) => {
    const { roomId, data: initData } = data;
    const room = rooms.get(roomId);
    if (room) {
      room.initSegment = initData;  // Cache for new clients
      socket.to(roomId).emit("stream-init", initData);
      console.log(`[STREAM] Init segment cached for room ${roomId}`);
    }
  });

  // === H.264 STREAM: Media segments (moof + mdat) ===
  socket.on("stream-data", (data) => {
    const { roomId, data: segmentData } = data;
    socket.to(roomId).emit("stream-data", segmentData);
  });

  // === LEGACY: MJPEG frame relay (backwards compatible) ===
  socket.on("screen-frame", (data) => {
    const { roomId, frame } = data;
    socket.to(roomId).emit("screen-frame", frame);
  });

  // === MOUSE/KEYBOARD RELAY ===
  socket.on("mouse-action", (data) => {
    const { roomId, action } = data;
    const room = rooms.get(roomId);
    if (room) io.to(room.host).emit("mouse-action", action);
  });

  socket.on("keyboard-action", (data) => {
    const { roomId, action } = data;
    const room = rooms.get(roomId);
    if (room) io.to(room.host).emit("keyboard-action", action);
  });

  // === CLEANUP ===
  socket.on("disconnect", () => {
    for (const [roomId, room] of rooms.entries()) {
      if (room.host === socket.id) {
        io.to(roomId).emit("host-disconnected");
        rooms.delete(roomId);
        console.log(`[ROOM] Deleted: ${roomId} (host left)`);
      } else if (room.clients.has(socket.id)) {
        room.clients.delete(socket.id);
        io.to(room.host).emit("client-left", socket.id);
      }
    }
    console.log(`[-] ${socket.id} disconnected`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 Remote Control Server v4 (H.264) on port ${PORT}`);
  console.log(`   Web Client: http://localhost:${PORT}`);
  console.log(`   Health: http://localhost:${PORT}/health\n`);
});
