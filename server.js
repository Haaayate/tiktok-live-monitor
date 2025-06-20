// server.js - TikTokライブ監視バックエンド
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const cors = require('cors');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// ミドルウェア設定
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ファイルアップロード設定
const upload = multer({ dest: 'uploads/' });

// データストレージ（本番環境ではデータベース使用推奨）
let monitoredUsers = new Map(); // username -> connection info
let connections = new Map(); // username -> WebcastPushConnection
let liveData = new Map(); // username -> live stats

// Socket.io接続管理
io.on('connection', (socket) => {
  console.log('クライアント接続:', socket.id);
  
  // 現在のライブデータを送信
  socket.emit('initial-data', Object.fromEntries(liveData));
  
  socket.on('disconnect', () => {
    console.log('クライアント切断:', socket.id);
  });
});

// TikTokライブ接続関数
async function connectToTikTokLive(username) {
  try {
    const tiktokLiveConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: true,
    });

    // 接続イベント
    tiktokLiveConnection.connect().then(state => {
      console.log(`${username}: 接続成功`);
      
      // 初期データ設定
      liveData.set(username, {
        username: username,
        isLive: true,
        viewerCount: 0,
        totalComments: 0,
        totalGifts: 0,
        totalDiamonds: 0,
        lastUpdate: new Date().toISOString(),
        recentComments: [],
        recentGifts: []
      });
      
      // データをクライアントに送信
      io.emit('user-connected', { username, status: 'connected' });
      io.emit('live-data-update', { username, data: liveData.get(username) });
    }).catch(err => {
      console.error(`${username}: 接続エラー`, err);
      io.emit('user-error', { username, error: err.message });
    });

    // コメントイベント
    tiktokLiveConnection.on('comment', data => {
      const userData = liveData.get(username);
      userData.totalComments++;
      userData.recentComments.unshift({
        user: data.nickname,
        comment: data.comment,
        timestamp: new Date().toISOString()
      });
      
      // 最新10件のコメントのみ保持
      userData.recentComments = userData.recentComments.slice(0, 10);
      userData.lastUpdate = new Date().toISOString();
      
      liveData.set(username, userData);
      
      // リアルタイム送信
      io.emit('new-comment', { username, data: {
        user: data.nickname,
        comment: data.comment,
        timestamp: userData.lastUpdate
      }});
      io.emit('live-data-update', { username, data: userData });
    });

    // ギフトイベント
    tiktokLiveConnection.on('gift', data => {
      const userData = liveData.get(username);
      userData.totalGifts++;
      
      // ダイヤモンド計算（概算）
      const diamondValue = data.giftDetails?.diamond_count || data.repeatCount || 1;
      userData.totalDiamonds += diamondValue;
      
      userData.recentGifts.unshift({
        user: data.nickname,
        giftName: data.giftDetails?.name || 'Unknown Gift',
        giftId: data.giftId,
        count: data.repeatCount,
        diamonds: diamondValue,
        timestamp: new Date().toISOString()
      });
      
      // 最新10件のギフトのみ保持
      userData.recentGifts = userData.recentGifts.slice(0, 10);
      userData.lastUpdate = new Date().toISOString();
      
      liveData.set(username, userData);
      
      // リアルタイム送信
      io.emit('new-gift', { username, data: userData.recentGifts[0] });
      io.emit('live-data-update', { username, data: userData });
    });

    // 視聴者数更新
    tiktokLiveConnection.on('roomUser', data => {
      const userData = liveData.get(username);
      userData.viewerCount = data.viewerCount || 0;
      userData.lastUpdate = new Date().toISOString();
      
      liveData.set(username, userData);
      io.emit('live-data-update', { username, data: userData });
    });

    // フォローイベント
    tiktokLiveConnection.on('follow', data => {
      io.emit('new-follow', { username, data: {
        user: data.nickname,
        timestamp: new Date().toISOString()
      }});
    });

    // 切断イベント
    tiktokLiveConnection.on('disconnected', () => {
      console.log(`${username}: 切断`);
      const userData = liveData.get(username);
      if (userData) {
        userData.isLive = false;
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
      }
      io.emit('user-disconnected', { username });
    });

    // エラーイベント
    tiktokLiveConnection.on('error', err => {
      console.error(`${username}: エラー`, err);
      io.emit('user-error', { username, error: err.message });
    });

    connections.set(username, tiktokLiveConnection);
    return tiktokLiveConnection;

  } catch (error) {
    console.error(`${username}: 接続作成エラー`, error);
    throw error;
  }
}

// API エンドポイント

// ユーザー追加
app.post('/api/add-user', async (req, res) => {
  const { username } = req.body;
  
  if (!username) {
    return res.status(400).json({ error: 'ユーザー名が必要です' });
  }
  
  // @を除去
  const cleanUsername = username.replace('@', '');
  
  if (monitoredUsers.has(cleanUsername)) {
    return res.status(400).json({ error: 'このユーザーは既に監視中です' });
  }
  
  try {
    await connectToTikTokLive(cleanUsername);
    monitoredUsers.set(cleanUsername, {
      username: cleanUsername,
      addedAt: new Date().toISOString(),
      status: 'monitoring'
    });
    
    res.json({ message: `${cleanUsername} の監視を開始しました` });
  } catch (error) {
    res.status(500).json({ error: `接続エラー: ${error.message}` });
  }
});

// ユーザー削除
app.post('/api/remove-user', (req, res) => {
  const { username } = req.body;
  const cleanUsername = username.replace('@', '');
  
  if (!monitoredUsers.has(cleanUsername)) {
    return res.status(404).json({ error: 'ユーザーが見つかりません' });
  }
  
  // 接続を切断
  const connection = connections.get(cleanUsername);
  if (connection) {
    connection.disconnect();
    connections.delete(cleanUsername);
  }
  
  monitoredUsers.delete(cleanUsername);
  liveData.delete(cleanUsername);
  
  io.emit('user-removed', { username: cleanUsername });
  res.json({ message: `${cleanUsername} の監視を停止しました` });
});

// CSV一括追加
app.post('/api/upload-csv', upload.single('csvfile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'CSVファイルが必要です' });
  }
  
  const results = [];
  const errors = [];
  
  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('data', (data) => {
      // CSVの最初の列をユーザー名として扱う
      const username = Object.values(data)[0];
      if (username) {
        results.push(username.replace('@', ''));
      }
    })
    .on('end', async () => {
      // アップロードファイルを削除
      fs.unlinkSync(req.file.path);
      
      // 各ユーザーに接続を試行
      for (const username of results) {
        if (!monitoredUsers.has(username)) {
          try {
            await connectToTikTokLive(username);
            monitoredUsers.set(username, {
              username: username,
              addedAt: new Date().toISOString(),
              status: 'monitoring'
            });
          } catch (error) {
            errors.push(`${username}: ${error.message}`);
          }
        }
      }
      
      res.json({ 
        message: `${results.length - errors.length}件のユーザーを追加しました`,
        errors: errors
      });
    });
});

// 監視ユーザー一覧取得
app.get('/api/users', (req, res) => {
  res.json({
    users: Array.from(monitoredUsers.values()),
    liveData: Object.fromEntries(liveData)
  });
});

// ライブデータ取得
app.get('/api/live-data', (req, res) => {
  res.json(Object.fromEntries(liveData));
});

// ヘルスチェック
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    monitoredUsers: monitoredUsers.size,
    activeConnections: connections.size
  });
});

// サーバー起動
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TikTokライブ監視サーバーが起動しました: http://localhost:${PORT}`);
});

// 終了時の処理
process.on('SIGTERM', () => {
  console.log('サーバーを停止しています...');
  
  // 全ての接続を切断
  connections.forEach((connection, username) => {
    console.log(`${username} の接続を切断中...`);
    connection.disconnect();
  });
  
  server.close(() => {
    console.log('サーバーが停止しました');
    process.exit(0);
  });
});
