// server.js - 一時的にPostgreSQLなし版
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

// データストレージ（メモリのみ）
let monitoredUsers = new Map();
let connections = new Map();
let liveData = new Map();

// Socket.io接続管理
io.on('connection', (socket) => {
  console.log('クライアント接続:', socket.id);
  socket.emit('initial-data', Object.fromEntries(liveData));
  
  socket.on('disconnect', () => {
    console.log('クライアント切断:', socket.id);
  });
});

// 初期ユーザーデータ作成関数
function createInitialUserData(username) {
  return {
    username: username,
    isLive: true,
    viewerCount: 0,
    totalComments: 0,
    totalGifts: 0,
    totalDiamonds: 0,
    lastUpdate: new Date().toISOString(),
    recentComments: [],
    recentGifts: []
  };
}

// 単一ユーザーのライブ状態チェック関数
async function checkSingleUserLiveStatus(username) {
  try {
    console.log(`${username}: 即座ライブ状態チェック開始`);
    
    const testConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: false,
    });
    
    await testConnection.connect();
    console.log(`${username}: ライブ配信中を確認（即座チェック）`);
    testConnection.disconnect();
    return true;
    
  } catch (error) {
    if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
      console.log(`${username}: ライブ終了を検出（即座チェック）`);
      
      const userData = liveData.get(username);
      if (userData) {
        userData.isLive = false;
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        io.emit('user-disconnected', { username });
        io.emit('live-data-update', { username, data: userData });
      }
      
      return false;
    } else {
      console.log(`${username}: 即座チェック中にエラー`, error.message);
      return null;
    }
  }
}

// TikTokライブ接続関数
async function connectToTikTokLive(username) {
  try {
    const tiktokLiveConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: true,
    });

    tiktokLiveConnection.connect().then(async state => {
      console.log(`${username}: 接続成功`);
      
      const initialData = createInitialUserData(username);
      liveData.set(username, initialData);
      
      io.emit('user-connected', { username, status: 'connected' });
      io.emit('live-data-update', { username, data: initialData });
      
      setTimeout(async () => {
        await checkSingleUserLiveStatus(username);
      }, 5000);
      
    }).catch(err => {
      console.error(`${username}: 接続エラー`, err);
      liveData.delete(username);
      io.emit('user-error', { username, error: err.message });
      throw err;
    });

    // イベントリスナー（省略版）
    tiktokLiveConnection.on('comment', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.totalComments++;
        userData.recentComments.unshift({
          user: data.nickname,
          comment: data.comment,
          timestamp: new Date().toISOString()
        });
        
        userData.recentComments = userData.recentComments.slice(0, 10);
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        io.emit('new-comment', { username, data: {
          user: data.nickname,
          comment: data.comment,
          timestamp: userData.lastUpdate
        }});
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: コメント処理エラー`, error);
      }
    });

    tiktokLiveConnection.on('gift', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.totalGifts++;
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
        
        userData.recentGifts = userData.recentGifts.slice(0, 10);
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        io.emit('new-gift', { username, data: userData.recentGifts[0] });
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: ギフト処理エラー`, error);
      }
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
    
    setTimeout(async () => {
      await checkSingleUserLiveStatus(cleanUsername);
    }, 10000);
    
    res.json({ message: `${cleanUsername} の監視を開始しました` });
  } catch (error) {
    let errorMessage = `接続エラー: ${error.message}`;
    if (error.message.includes('LIVE has ended')) {
      errorMessage = `${cleanUsername} は現在ライブ配信をしていません`;
    } else if (error.message.includes('UserOfflineError')) {
      errorMessage = `${cleanUsername} はオフラインです`;
    }
    
    res.status(500).json({ error: errorMessage });
  }
});

// CSV一括追加
app.post('/api/upload-csv', upload.single('csvfile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'CSVファイルが必要です' });
  }
  
  const errors = [];
  const successUsers = [];
  
  try {
    const csvData = await new Promise((resolve, reject) => {
      const data = [];
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (row) => {
          const username = Object.values(row)[0];
          if (username && username.trim()) {
            data.push(username.replace('@', '').trim());
          }
        })
        .on('end', () => resolve(data))
        .on('error', reject);
    });
    
    fs.unlinkSync(req.file.path);
    
    for (const username of csvData) {
      try {
        if (monitoredUsers.has(username)) {
          errors.push(`${username}: 既に監視中です`);
          continue;
        }
        
        await connectToTikTokLive(username);
        monitoredUsers.set(username, {
          username: username,
          addedAt: new Date().toISOString(),
          status: 'monitoring'
        });
        
        successUsers.push(username);
        
        setTimeout(async () => {
          await checkSingleUserLiveStatus(username);
        }, 15000 + (successUsers.length * 1000));
        
      } catch (error) {
        let errorMessage = error.message;
        if (error.message.includes('LIVE has ended')) {
          errorMessage = '現在ライブ配信していません';
        } else if (error.message.includes('UserOfflineError')) {
          errorMessage = 'オフラインです';
        }
        
        errors.push(`${username}: ${errorMessage}`);
      }
    }
    
    res.json({ 
      message: `${successUsers.length}件のユーザーを追加しました`,
      success: successUsers.length,
      total: csvData.length,
      errors: errors.length > 0 ? errors : undefined
    });
    
  } catch (error) {
    if (fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'CSVファイルの処理中にエラーが発生しました: ' + error.message 
    });
  }
});

// ユーザー削除
app.post('/api/remove-user', (req, res) => {
  const { username } = req.body;
  const cleanUsername = username.replace('@', '');
  
  if (!monitoredUsers.has(cleanUsername)) {
    return res.status(404).json({ error: 'ユーザーが見つかりません' });
  }
  
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

// 監視ユーザー一覧取得
app.get('/api/users', (req, res) => {
  res.json({
    users: Array.from(monitoredUsers.values()),
    liveData: Object.fromEntries(liveData)
  });
});

// ランキング取得
app.get('/api/ranking', (req, res) => {
  const users = Array.from(liveData.values());
  
  const dailyRanking = users
    .filter(user => user.totalDiamonds > 0)
    .sort((a, b) => b.totalDiamonds - a.totalDiamonds)
    .map((user, index) => ({
      rank: index + 1,
      username: user.username,
      totalDiamonds: user.totalDiamonds,
      totalGifts: user.totalGifts,
      totalComments: user.totalComments,
      viewerCount: user.viewerCount,
      isLive: user.isLive,
      estimatedEarnings: Math.round(user.totalDiamonds * 0.005 * 100) / 100,
      lastUpdate: user.lastUpdate
    }));
  
  res.json({
    ranking: dailyRanking,
    totalUsers: users.length,
    activeUsers: dailyRanking.length,
    lastUpdate: new Date().toISOString()
  });
});

// ルート設定
app.get('/', (req, res) => {
  res.json({ 
    status: 'TikTok Live Monitor API - No Database Version',
    timestamp: new Date().toISOString(),
    version: '1.5.0'
  });
});

// ヘルスチェック
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    database: 'disabled',
    monitoredUsers: monitoredUsers.size,
    activeConnections: connections.size
  });
});

// 定期的なライブ状態チェック（3分ごと）
setInterval(() => {
  checkLiveStatus();
}, 3 * 60 * 1000);

async function checkLiveStatus() {
  console.log('=== ライブ状態チェック開始 ===');
  
  for (const [username, userData] of liveData) {
    if (userData.isLive) {
      try {
        const testConnection = new WebcastPushConnection(username, {
          enableExtendedGiftInfo: false,
        });
        
        await testConnection.connect();
        console.log(`${username}: ライブ配信中を確認`);
        testConnection.disconnect();
        
      } catch (error) {
        if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
          console.log(`${username}: ライブ終了を検出、オフライン設定`);
          
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
          
          const existingConnection = connections.get(username);
          if (existingConnection) {
            existingConnection.disconnect();
          }
          
          io.emit('user-disconnected', { username });
          io.emit('live-data-update', { username, data: userData });
        }
      }
    }
  }
  
  console.log('=== ライブ状態チェック完了 ===');
}

// サーバー起動
const PORT = process.env.PORT || 10000;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`=== TikTok Live Monitor Server (No Database) ===`);
  console.log(`Server running on port ${PORT}`);
  console.log(`CSV Upload: Enabled`);
});

// エラーハンドリング
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

// 終了時の処理
process.on('SIGTERM', () => {
  console.log('サーバーを停止しています...');
  
  connections.forEach((connection, username) => {
    connection.disconnect();
  });
  
  server.close(() => {
    console.log('サーバーが停止しました');
    process.exit(0);
  });
});
