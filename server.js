// server.js - TikTokライブ監視バックエンド（修正版）
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
    
    // 新しい接続を試して確認
    const testConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: false,
    });
    
    await testConnection.connect();
    console.log(`${username}: ライブ配信中を確認（即座チェック）`);
    
    // 接続成功した場合はライブ中なので何もしない
    testConnection.disconnect();
    return true; // ライブ中
    
  } catch (error) {
    if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
      console.log(`${username}: ライブ終了を検出（即座チェック）`);
      
      // liveDataが存在する場合はオフライン設定
      const userData = liveData.get(username);
      if (userData) {
        userData.isLive = false;
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        // フロントエンドに通知
        io.emit('user-disconnected', { username });
        io.emit('live-data-update', { username, data: userData });
      }
      
      return false; // オフライン
    } else {
      console.log(`${username}: 即座チェック中にエラー`, error.message);
      return null; // 不明
    }
  }
}

// TikTokライブ接続関数
async function connectToTikTokLive(username) {
  try {
    const tiktokLiveConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: true,
    });

    // 接続イベント
    tiktokLiveConnection.connect().then(async state => {
      console.log(`${username}: 接続成功`);
      
      // 初期データ設定
      const initialData = createInitialUserData(username);
      liveData.set(username, initialData);
      
      // データをクライアントに送信
      io.emit('user-connected', { username, status: 'connected' });
      io.emit('live-data-update', { username, data: initialData });
      
      // 接続成功後、5秒待ってから即座にライブ状態をチェック
      setTimeout(async () => {
        console.log(`${username}: 接続後の即座ライブ状態チェック開始`);
        await checkSingleUserLiveStatus(username);
      }, 5000);
      
    }).catch(err => {
      console.error(`${username}: 接続エラー`, err);
      
      // 接続失敗時はliveDataからも削除
      liveData.delete(username);
      
      // エラー情報をフロントエンドに送信
      io.emit('user-error', { username, error: err.message });
      
      // 接続失敗時は例外を投げる
      throw err;
    });

    // コメントイベント
    tiktokLiveConnection.on('comment', data => {
      try {
        let userData = liveData.get(username);
        if (!userData) {
          userData = createInitialUserData(username);
        }
        
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
      } catch (error) {
        console.error(`${username}: コメント処理エラー`, error);
      }
    });

    // ギフトイベント
    tiktokLiveConnection.on('gift', data => {
      try {
        let userData = liveData.get(username);
        if (!userData) {
          userData = createInitialUserData(username);
        }
        
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
      } catch (error) {
        console.error(`${username}: ギフト処理エラー`, error);
      }
    });

    // 視聴者数更新（修正版）
    tiktokLiveConnection.on('roomUser', data => {
      try {
        let userData = liveData.get(username);
        if (!userData) {
          userData = createInitialUserData(username);
        }
        
        userData.viewerCount = data.viewerCount || 0;
        userData.lastUpdate = new Date().toISOString();
        
        liveData.set(username, userData);
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: 視聴者数更新エラー`, error);
      }
    });

    // フォローイベント
    tiktokLiveConnection.on('follow', data => {
      try {
        io.emit('new-follow', { username, data: {
          user: data.nickname,
          timestamp: new Date().toISOString()
        }});
      } catch (error) {
        console.error(`${username}: フォロー処理エラー`, error);
      }
    });

    // 切断イベント
    tiktokLiveConnection.on('disconnected', () => {
      console.log(`${username}: 切断`);
      try {
        const userData = liveData.get(username);
        if (userData) {
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
        }
        io.emit('user-disconnected', { username });
      } catch (error) {
        console.error(`${username}: 切断処理エラー`, error);
      }
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
    
    console.log(`${cleanUsername}: ユーザー追加完了、即座ライブ状態チェックを開始します`);
    
    // ユーザー追加後、10秒待ってから即座にライブ状態をチェック
    setTimeout(async () => {
      console.log(`${cleanUsername}: ユーザー追加後の即座ライブ状態チェック`);
      await checkSingleUserLiveStatus(cleanUsername);
    }, 10000);
    
    res.json({ message: `${cleanUsername} の監視を開始しました` });
  } catch (error) {
    // 接続エラーの場合、ユーザーデータも作成しない
    console.error(`${cleanUsername}: ユーザー追加失敗`, error);
    
    // エラーの種類に応じたメッセージ
    let errorMessage = `接続エラー: ${error.message}`;
    if (error.message.includes('LIVE has ended')) {
      errorMessage = `${cleanUsername} は現在ライブ配信をしていません`;
    } else if (error.message.includes('UserOfflineError')) {
      errorMessage = `${cleanUsername} はオフラインです`;
    }
    
    res.status(500).json({ error: errorMessage });
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
            
            // CSV追加後も即座ライブ状態チェック（15秒待機）
            setTimeout(async () => {
              console.log(`${username}: CSV追加後の即座ライブ状態チェック`);
              await checkSingleUserLiveStatus(username);
            }, 15000);
            
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

// ランキング取得
app.get('/api/ranking', (req, res) => {
  try {
    // ライブデータから配列を作成
    const users = Array.from(liveData.values());
    
    // ダイヤモンド数でソート（降順）
    const dailyRanking = users
      .filter(user => user.totalDiamonds > 0) // ダイヤモンドがあるユーザーのみ
      .sort((a, b) => b.totalDiamonds - a.totalDiamonds)
      .map((user, index) => ({
        rank: index + 1,
        username: user.username,
        totalDiamonds: user.totalDiamonds,
        totalGifts: user.totalGifts,
        totalComments: user.totalComments,
        viewerCount: user.viewerCount,
        isLive: user.isLive,
        estimatedEarnings: Math.round(user.totalDiamonds * 0.005 * 100) / 100, // $5/1000ダイヤモンド
        lastUpdate: user.lastUpdate
      }));
    
    res.json({
      ranking: dailyRanking,
      totalUsers: users.length,
      activeUsers: dailyRanking.length,
      lastUpdate: new Date().toISOString()
    });
  } catch (error) {
    console.error('ランキング取得エラー:', error);
    res.status(500).json({ error: 'ランキング取得に失敗しました' });
  }
});

// ライブデータ取得
app.get('/api/live-data', (req, res) => {
  res.json(Object.fromEntries(liveData));
});

// ルート設定
app.get('/', (req, res) => {
  res.json({ 
    status: 'TikTok Live Monitor API',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
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

// 定期的なライブ状態チェック（3分ごと）
setInterval(() => {
  checkLiveStatus();
}, 3 * 60 * 1000);

// ライブ状態チェック関数
async function checkLiveStatus() {
  console.log('=== ライブ状態チェック開始 ===');
  
  for (const [username, userData] of liveData) {
    if (userData.isLive) {
      try {
        // 新しい接続を試して確認
        const testConnection = new WebcastPushConnection(username, {
          enableExtendedGiftInfo: false,
        });
        
        await testConnection.connect();
        console.log(`${username}: ライブ配信中を確認`);
        
        // 接続成功した場合はライブ中なので何もしない
        testConnection.disconnect();
        
      } catch (error) {
        if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
          console.log(`${username}: ライブ終了を検出、オフライン設定`);
          
          // オフライン設定
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
          
          // 既存の接続があれば切断
          const existingConnection = connections.get(username);
          if (existingConnection) {
            existingConnection.disconnect();
          }
          
          // フロントエンドに通知
          io.emit('user-disconnected', { username });
          io.emit('live-data-update', { username, data: userData });
          
        } else {
          console.log(`${username}: チェック中にエラー`, error.message);
        }
      }
    }
  }
  
  console.log('=== ライブ状態チェック完了 ===');
}

// サーバー起動
const PORT = process.env.PORT || 10000;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`=== TikTok Live Monitor Server ===`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log(`Health check: /health`);
  console.log(`API Base: /api`);
});

// エラーハンドリング
server.on('error', (err) => {
  console.error('Server error:', err);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
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
