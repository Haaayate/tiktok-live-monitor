// server.js - TikTokライブ監視バックエンド（PostgreSQL + CSV完全版）
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const cors = require('cors');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// PostgreSQL接続設定
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ミドルウェア設定
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ファイルアップロード設定
const upload = multer({ dest: 'uploads/' });

// 接続管理設定
const MAX_CONCURRENT_CONNECTIONS = 25; // 最大同時接続数を25に設定
const CONNECTION_RETRY_DELAY = 5000; // 接続リトライ間隔

// データストレージ（メモリ + DB）
let connections = new Map();
let liveData = new Map();
let connectionQueue = []; // 接続待機キュー

// データベース初期化
async function initializeDatabase() {
  try {
    console.log('データベース初期化開始...');
    
    // テーブル作成
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(50) DEFAULT 'monitoring',
        total_diamonds INTEGER DEFAULT 0,
        total_gifts INTEGER DEFAULT 0,
        total_comments INTEGER DEFAULT 0,
        last_live_check TIMESTAMP,
        is_live BOOLEAN DEFAULT false,
        viewer_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // ライブデータ履歴テーブル
    await pool.query(`
      CREATE TABLE IF NOT EXISTS live_history (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        diamonds INTEGER DEFAULT 0,
        gifts INTEGER DEFAULT 0,
        comments INTEGER DEFAULT 0,
        viewers INTEGER DEFAULT 0,
        is_live BOOLEAN DEFAULT false,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // インデックス作成
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
      CREATE INDEX IF NOT EXISTS idx_live_history_username ON live_history(username);
      CREATE INDEX IF NOT EXISTS idx_live_history_recorded_at ON live_history(recorded_at);
    `);
    
    console.log('データベース初期化完了');
    
    // 既存ユーザーの復元
    await restoreExistingUsers();
    
  } catch (error) {
    console.error('データベース初期化エラー:', error);
  }
}

// 既存ユーザーの復元
async function restoreExistingUsers() {
  try {
    console.log('既存ユーザーの復元開始...');
    
    const result = await pool.query(`
      SELECT username, total_diamonds, total_gifts, total_comments, 
             is_live, viewer_count, last_live_check, status
      FROM users 
      WHERE status IN ('monitoring', 'waiting')
      ORDER BY added_at ASC
    `);
    
    console.log(`${result.rows.length}件のユーザーを復元中...`);
    
    for (const user of result.rows) {
      console.log(`${user.username}: 復元開始 (status: ${user.status})`);
      
      // liveDataに復元（必ず作成）
      const userData = {
        username: user.username,
        isLive: user.is_live || false,
        viewerCount: user.viewer_count || 0,
        totalComments: user.total_comments || 0,
        totalGifts: user.total_gifts || 0,
        totalDiamonds: user.total_diamonds || 0,
        lastUpdate: user.last_live_check || new Date().toISOString(),
        recentComments: [],
        recentGifts: []
      };
      
      liveData.set(user.username, userData);
      console.log(`${user.username}: liveDataに復元完了`);
      
      // 接続数制限チェック
      if (user.status === 'monitoring' && connections.size < MAX_CONCURRENT_CONNECTIONS) {
        try {
          await connectToTikTokLive(user.username);
          console.log(`${user.username}: TikTok接続復元成功`);
        } catch (error) {
          console.log(`${user.username}: TikTok接続復元失敗 - ${error.message}`);
          // 接続失敗してもliveDataは保持
          // 接続失敗時は待機キューに追加
          connectionQueue.push(user.username);
          await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', user.username]);
        }
      } else {
        // 接続制限により待機キューに追加
        connectionQueue.push(user.username);
        await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', user.username]);
        console.log(`${user.username}: 接続制限により待機キューに追加`);
      }
    }
    
    console.log('既存ユーザーの復元完了');
    console.log(`liveData件数: ${liveData.size}`);
    console.log(`アクティブ接続: ${connections.size}`);
    console.log(`待機キュー: ${connectionQueue.length}`);
    
  } catch (error) {
    console.error('ユーザー復元エラー:', error);
  }
}

// 接続キュー処理
async function processConnectionQueue() {
  if (connectionQueue.length === 0) return;
  if (connections.size >= MAX_CONCURRENT_CONNECTIONS) return;
  
  const username = connectionQueue.shift();
  console.log(`キューから接続処理: ${username}`);
  
  try {
    await connectToTikTokLive(username);
    
    // データベースのステータスを更新
    await pool.query(`
      UPDATE users SET status = 'monitoring', updated_at = CURRENT_TIMESTAMP
      WHERE username = $1
    `, [username]);
    
    console.log(`${username}: キューからの接続成功`);
    
    // 通知送信
    io.emit('user-connected', { username, status: 'connected' });
    
    // 次のキューを処理
    setTimeout(() => {
      processConnectionQueue();
    }, CONNECTION_RETRY_DELAY);
    
  } catch (error) {
    console.error(`${username}: キューからの接続失敗`, error);
    
    // 失敗した場合は再度キューに追加
    connectionQueue.push(username);
  }
}

// 定期的なキュー処理
setInterval(() => {
  processConnectionQueue();
}, 30000); // 30秒ごと

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

// データベースにユーザーデータ保存
async function saveUserToDatabase(username, userData) {
  try {
    await pool.query(`
      INSERT INTO users (username, total_diamonds, total_gifts, total_comments, 
                        is_live, viewer_count, last_live_check)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (username) 
      DO UPDATE SET 
        total_diamonds = $2,
        total_gifts = $3,
        total_comments = $4,
        is_live = $5,
        viewer_count = $6,
        last_live_check = $7,
        updated_at = CURRENT_TIMESTAMP
    `, [
      username,
      userData.totalDiamonds || 0,
      userData.totalGifts || 0,
      userData.totalComments || 0,
      userData.isLive || false,
      userData.viewerCount || 0,
      new Date()
    ]);
  } catch (error) {
    console.error(`${username}: データベース保存エラー`, error);
  }
}

// ライブ履歴データ保存
async function saveLiveHistory(username, userData) {
  try {
    await pool.query(`
      INSERT INTO live_history (username, diamonds, gifts, comments, viewers, is_live)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      username,
      userData.totalDiamonds || 0,
      userData.totalGifts || 0,
      userData.totalComments || 0,
      userData.viewerCount || 0,
      userData.isLive || false
    ]);
  } catch (error) {
    console.error(`${username}: 履歴保存エラー`, error);
  }
}

// より正確なライブ状態チェック関数
async function checkSingleUserLiveStatusAccurate(username) {
  try {
    console.log(`${username}: 正確なライブ状態チェック開始`);
    
    const testConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: false,
      processInitialData: false,
      enableWebsocketUpgrade: true,
      requestPollingIntervalMs: 1000,
      sessionId: undefined,
      clientParams: {},
      requestHeaders: {},
      websocketHeaders: {},
      requestOptions: {},
      websocketOptions: {}
    });
    
    // タイムアウト付きで接続テスト
    const connectionPromise = testConnection.connect();
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Connection timeout')), 10000);
    });
    
    await Promise.race([connectionPromise, timeoutPromise]);
    
    // 短時間待機してライブストリームの状態を確認
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log(`${username}: ライブ配信中を確認（高精度チェック）`);
    testConnection.disconnect();
    
    return true;
    
  } catch (error) {
    console.log(`${username}: ライブ状態チェックエラー - ${error.message}`);
    
    // オフライン判定に該当するエラーパターンを追加
    const offlineErrors = [
      'LIVE has ended',
      'UserOfflineError',
      'User is not live',
      'Room not found',
      'Connection timeout',
      'Failed to retrieve the initial room data',  // 新しく追加
      'Failed to connect to websocket',
      'Unable to retrieve room data',
      'Room is not available',
      'Stream is not available',
      'User does not exist',
      'Private account or user not found'
    ];
    
    const isOffline = offlineErrors.some(pattern => 
      error.message.includes(pattern)
    );
    
    if (isOffline) {
      console.log(`${username}: オフライン状態を確認 (エラー: ${error.message})`);
      return false;
    }
    
    // 不明なエラーの場合はnullを返す
    console.log(`${username}: 判定不能なエラー - ${error.message}`);
    return null;
  }
}

// 単一ユーザーのライブ状態チェック関数（既存）
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
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
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

    // 接続イベント
    tiktokLiveConnection.connect().then(async state => {
      console.log(`${username}: 接続成功`);
      
      const initialData = createInitialUserData(username);
      liveData.set(username, initialData);
      
      // データベースに保存
      await saveUserToDatabase(username, initialData);
      
      io.emit('user-connected', { username, status: 'connected' });
      io.emit('live-data-update', { username, data: initialData });
      
      setTimeout(async () => {
        console.log(`${username}: 接続後の即座ライブ状態チェック開始`);
        await checkSingleUserLiveStatus(username);
      }, 5000);
      
    }).catch(err => {
      console.error(`${username}: 接続エラー`, err);
      liveData.delete(username);
      io.emit('user-error', { username, error: err.message });
      throw err;
    });

    // コメントイベント
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
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
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
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        io.emit('new-gift', { username, data: userData.recentGifts[0] });
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: ギフト処理エラー`, error);
      }
    });

    // 視聴者数更新
    tiktokLiveConnection.on('roomUser', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.viewerCount = data.viewerCount || 0;
        userData.lastUpdate = new Date().toISOString();
        
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
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
    tiktokLiveConnection.on('disconnected', async () => {
      console.log(`${username}: 切断`);
      try {
        const userData = liveData.get(username);
        if (userData) {
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
          
          // データベースに保存
          await saveUserToDatabase(username, userData);
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


// =============================================================================
// TikTok接続デバッグ機能 - 追加開始
// =============================================================================

// より詳細なデバッグ付きTikTok接続テスト
async function debugTikTokConnection(username) {
    console.log(`🐛 [${username}] TikTok接続デバッグ開始`);
    console.log(`📦 tiktok-live-connector バージョン: ${require('tiktok-live-connector/package.json').version}`);
    
    const results = {
        username,
        timestamp: new Date().toISOString(),
        libraryVersion: require('tiktok-live-connector/package.json').version,
        attempts: [],
        systemInfo: {
            nodeVersion: process.version,
            platform: process.platform,
            env: process.env.NODE_ENV
        }
    };
    
    // 試行1: 最小設定での接続
    try {
        console.log(`🔍 [${username}] 最小設定テスト...`);
        
        const connection = new WebcastPushConnection(username, {
            enableExtendedGiftInfo: false,
            processInitialData: false,
            enableWebsocketUpgrade: false,
            requestPollingIntervalMs: 2000,
            requestOptions: {
                timeout: 10000
            }
        });
        
        // 詳細なイベントリスナーを追加
        connection.on('connect', (state) => {
            console.log(`✅ [${username}] 接続イベント発生:`, state);
        });
        
        connection.on('error', (error) => {
            console.log(`❌ [${username}] エラーイベント:`, error);
        });
        
        connection.on('disconnect', () => {
            console.log(`🔌 [${username}] 切断イベント`);
        });
        
        // タイムアウト付きで接続
        const connectPromise = connection.connect();
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('接続タイムアウト (10秒)')), 10000);
        });
        
        const connectResult = await Promise.race([connectPromise, timeoutPromise]);
        
        console.log(`✅ [${username}] 最小設定接続成功:`, connectResult);
        
        results.attempts.push({
            method: 'minimal_config',
            result: 'success',
            connectResult: connectResult,
            message: 'ライブ配信中（最小設定）'
        });
        
        // 少し待ってから切断
        setTimeout(() => {
            connection.disconnect();
        }, 2000);
        
        return { isLive: true, details: results };
        
    } catch (error) {
        console.log(`❌ [${username}] 最小設定エラー:`, error);
        console.log(`📊 [${username}] エラー詳細:`, {
            name: error.name,
            message: error.message,
            stack: error.stack,
            code: error.code
        });
        
        results.attempts.push({
            method: 'minimal_config',
            result: 'error',
            error: {
                name: error.name,
                message: error.message,
                code: error.code
            }
        });
    }
    
    // 試行2: 異なるユーザーエージェント設定
    try {
        console.log(`🔍 [${username}] カスタムヘッダーテスト...`);
        
        const connection = new WebcastPushConnection(username, {
            enableExtendedGiftInfo: false,
            requestHeaders: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            requestOptions: {
                timeout: 15000
            }
        });
        
        await connection.connect();
        console.log(`✅ [${username}] カスタムヘッダー接続成功`);
        
        results.attempts.push({
            method: 'custom_headers',
            result: 'success',
            message: 'ライブ配信中（カスタムヘッダー）'
        });
        
        setTimeout(() => {
            connection.disconnect();
        }, 2000);
        
        return { isLive: true, details: results };
        
    } catch (error) {
        console.log(`❌ [${username}] カスタムヘッダーエラー:`, error.message);
        
        results.attempts.push({
            method: 'custom_headers',
            result: 'error',
            error: {
                message: error.message,
                code: error.code
            }
        });
    }
    
    // 試行3: WebSocket無効化テスト
    try {
        console.log(`🔍 [${username}] WebSocket無効化テスト...`);
        
        const connection = new WebcastPushConnection(username, {
            enableExtendedGiftInfo: false,
            enableWebsocketUpgrade: false,
            processInitialData: true,
            requestPollingIntervalMs: 1000
        });
        
        await connection.connect();
        console.log(`✅ [${username}] WebSocket無効化接続成功`);
        
        results.attempts.push({
            method: 'no_websocket',
            result: 'success',
            message: 'ライブ配信中（WebSocket無効）'
        });
        
        setTimeout(() => {
            connection.disconnect();
        }, 2000);
        
        return { isLive: true, details: results };
        
    } catch (error) {
        console.log(`❌ [${username}] WebSocket無効化エラー:`, error.message);
        
        results.attempts.push({
            method: 'no_websocket',
            result: 'error',
            error: {
                message: error.message
            }
        });
    }
    
    console.log(`⚫ [${username}] 全ての接続方法が失敗`);
    return { isLive: false, details: results };
}

// デバッグテスト用APIエンドポイント
app.post('/api/debug-tiktok-connection', async (req, res) => {
    const { username } = req.body;
    
    if (!username) {
        return res.status(400).json({ error: 'ユーザー名が必要です' });
    }
    
    const cleanUsername = username.replace('@', '').trim();
    console.log(`🐛 TikTok接続デバッグ開始: ${cleanUsername}`);
    
    try {
        const result = await debugTikTokConnection(cleanUsername);
        
        console.log(`📊 デバッグ結果 [${cleanUsername}]:`, JSON.stringify(result, null, 2));
        
        res.json({
            success: true,
            username: cleanUsername,
            result: result,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error(`❌ デバッグエラー [${cleanUsername}]:`, error);
        res.status(500).json({ 
            error: `デバッグエラー: ${error.message}`,
            username: cleanUsername
        });
    }
});

// ライブラリ情報確認用エンドポイント
app.get('/api/library-info', (req, res) => {
    try {
        const packageInfo = require('tiktok-live-connector/package.json');
        
        res.json({
            success: true,
            library: {
                name: packageInfo.name,
                version: packageInfo.version,
                description: packageInfo.description,
                lastModified: packageInfo._time || 'unknown'
            },
            system: {
                nodeVersion: process.version,
                platform: process.platform,
                architecture: process.arch,
                environment: process.env.NODE_ENV || 'development'
            },
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        res.status(500).json({
            error: 'ライブラリ情報の取得に失敗しました',
            details: error.message
        });
    }
});

// =============================================================================
// TikTok接続デバッグ機能 - 追加終了
// =============================================================================


// デバッグテスト用APIエンドポイント
app.post('/api/debug-tiktok-connection', async (req, res) => {
    const { username } = req.body;
    
    if (!username) {
        return res.status(400).json({ error: 'ユーザー名が必要です' });
    }
    
    const cleanUsername = username.replace('@', '').trim();
    console.log(`🐛 TikTok接続デバッグ開始: ${cleanUsername}`);
    
    try {
        const result = await debugTikTokConnection(cleanUsername);
        
        console.log(`📊 デバッグ結果 [${cleanUsername}]:`, JSON.stringify(result, null, 2));
        
        res.json({
            success: true,
            username: cleanUsername,
            result: result,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error(`❌ デバッグエラー [${cleanUsername}]:`, error);
        res.status(500).json({ 
            error: `デバッグエラー: ${error.message}`,
            username: cleanUsername
        });
    }
});

// ライブラリ情報確認用エンドポイント
app.get('/api/library-info', (req, res) => {
    try {
        const packageInfo = require('tiktok-live-connector/package.json');
        
        res.json({
            success: true,
            library: {
                name: packageInfo.name,
                version: packageInfo.version,
                description: packageInfo.description,
                lastModified: packageInfo._time || 'unknown'
            },
            system: {
                nodeVersion: process.version,
                platform: process.platform,
                architecture: process.arch,
                environment: process.env.NODE_ENV || 'development'
            },
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        res.status(500).json({
            error: 'ライブラリ情報の取得に失敗しました',
            details: error.message
        });
    }
});

// =============================================================================
// TikTok接続デバッグ機能 - 追加終了
// =============================================================================

// ユーザー追加（完全修正版）
app.post('/api/add-user', async (req, res) => {
  const { username } = req.body;
  
  console.log('ユーザー追加リクエスト受信:', req.body);
  
  if (!username) {
    console.log('エラー: ユーザー名が空');
    return res.status(400).json({ error: 'ユーザー名が必要です' });
  }
  
  const cleanUsername = username.replace('@', '').trim();
  
  if (!cleanUsername) {
    console.log('エラー: 有効なユーザー名なし');
    return res.status(400).json({ error: '有効なユーザー名を入力してください' });
  }
  
  console.log(`${cleanUsername}: ユーザー追加処理開始`);
  
  try {
    // データベースで重複チェック
    console.log(`${cleanUsername}: 重複チェック開始`);
    const existingUser = await pool.query('SELECT username FROM users WHERE username = $1', [cleanUsername]);
    
    if (existingUser.rows.length > 0) {
      console.log(`${cleanUsername}: 既に存在`);
      return res.status(400).json({ error: 'このユーザーは既に監視中です' });
    }
    
    console.log(`${cleanUsername}: 重複なし、追加処理継続`);
    
    // まずliveDataに基本データを作成
    const userData = {
      username: cleanUsername,
      isLive: false,  // 初期状態はオフライン
      viewerCount: 0,
      totalComments: 0,
      totalGifts: 0,
      totalDiamonds: 0,
      lastUpdate: new Date().toISOString(),
      recentComments: [],
      recentGifts: []
    };
    
    liveData.set(cleanUsername, userData);
    console.log(`${cleanUsername}: liveDataに追加完了`);
    
    // データベースに追加
    const insertQuery = `
      INSERT INTO users (username, status, is_live, total_diamonds, total_gifts, total_comments, viewer_count, last_live_check)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `;
    
    const currentTime = new Date();
    
    await pool.query(insertQuery, [
      cleanUsername,
      'monitoring',  // 初期状態は monitoring
      false,         // 初期状態はオフライン
      0,            // total_diamonds
      0,            // total_gifts
      0,            // total_comments
      0,            // viewer_count
      currentTime   // last_live_check
    ]);
    
    console.log(`${cleanUsername}: データベースに追加完了`);
    
    // 接続数制限チェック
    if (connections.size >= MAX_CONCURRENT_CONNECTIONS) {
      console.log(`${cleanUsername}: 接続制限により待機キューに追加`);
      
      // ステータスを待機中に変更
      await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', cleanUsername]);
      
      connectionQueue.push(cleanUsername);
      
      // クライアントに通知
      io.emit('live-data-update', { username: cleanUsername, data: userData });
      
      return res.json({ 
        message: `${cleanUsername} を追加しました（接続待機中: ${connectionQueue.length}番目）`,
        status: 'waiting',
        queuePosition: connectionQueue.length
      });
    }
    
    // TikTok接続を試行
    console.log(`${cleanUsername}: TikTok接続試行開始`);
    
    try {
      await connectToTikTokLive(cleanUsername);
      console.log(`${cleanUsername}: TikTok接続成功`);
      
      // 接続成功の通知
      io.emit('user-connected', { username: cleanUsername, status: 'connected' });
      io.emit('live-data-update', { username: cleanUsername, data: userData });
      
      // 5秒後にライブ状態をチェック
      setTimeout(async () => {
        console.log(`${cleanUsername}: 追加後ライブ状態チェック開始`);
        try {
          const isLive = await checkSingleUserLiveStatusAccurate(cleanUsername);
          
          if (isLive !== null) {
            const currentUserData = liveData.get(cleanUsername);
            if (currentUserData) {
              currentUserData.isLive = isLive;
              currentUserData.lastUpdate = new Date().toISOString();
              liveData.set(cleanUsername, currentUserData);
              await saveUserToDatabase(cleanUsername, currentUserData);
              
              // 状態変更を通知
              io.emit('live-data-update', { username: cleanUsername, data: currentUserData });
              console.log(`${cleanUsername}: 初期ライブ状態設定完了 (${isLive ? 'ライブ中' : 'オフライン'})`);
            }
          }
        } catch (statusError) {
          console.error(`${cleanUsername}: 状態チェックエラー`, statusError);
        }
      }, 5000);
      
      res.json({ 
        message: `${cleanUsername} の監視を開始しました`,
        status: 'monitoring'
      });
      
    } catch (connectError) {
      console.log(`${cleanUsername}: TikTok接続失敗 - ${connectError.message}`);
      
      // 接続失敗でもユーザーは追加済み（監視状態のまま）
      // エラーメッセージを分かりやすく
      let errorMessage = connectError.message;
      if (connectError.message.includes('LIVE has ended') || 
          connectError.message.includes('Failed to retrieve the initial room data')) {
        errorMessage = '現在ライブ配信をしていません';
      } else if (connectError.message.includes('UserOfflineError')) {
        errorMessage = 'オフラインです';
      }
      
      // クライアントに通知
      io.emit('live-data-update', { username: cleanUsername, data: userData });
      
      res.json({
        message: `${cleanUsername} を追加しました（${errorMessage}）`,
        status: 'monitoring',
        warning: errorMessage
      });
    }
    
  } catch (error) {
    console.error(`${cleanUsername}: ユーザー追加エラー`, error);
    
    // エラー時はliveDataからも削除
    liveData.delete(cleanUsername);
    
    res.status(500).json({ error: `追加エラー: ${error.message}` });
  }
});

// ユーザー削除（接続解放対応）
app.post('/api/remove-user', async (req, res) => {
  const { username } = req.body;
  const cleanUsername = username.replace('@', '');
  
  try {
    // データベースから削除
    const result = await pool.query('DELETE FROM users WHERE username = $1 RETURNING *', [cleanUsername]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'ユーザーが見つかりません' });
    }
    
    // 接続を切断
    const connection = connections.get(cleanUsername);
    if (connection) {
      connection.disconnect();
      connections.delete(cleanUsername);
      console.log(`${cleanUsername}: 接続解放 (残り${connections.size})`);
    }
    
    liveData.delete(cleanUsername);
    
    // キューから削除（もしある場合）
    const queueIndex = connectionQueue.indexOf(cleanUsername);
    if (queueIndex !== -1) {
      connectionQueue.splice(queueIndex, 1);
    }
    
    io.emit('user-removed', { username: cleanUsername });
    
    // 接続枠が空いたのでキューを処理
    setTimeout(() => {
      processConnectionQueue();
    }, 1000);
    
    res.json({ 
      message: `${cleanUsername} の監視を停止しました`,
      activeConnections: connections.size,
      queueLength: connectionQueue.length
    });
  } catch (error) {
    console.error(`${cleanUsername}: 削除エラー`, error);
    res.status(500).json({ error: '削除に失敗しました' });
  }
});

// CSV一括追加（データベース対応版）
app.post('/api/upload-csv', upload.single('csvfile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'CSVファイルが必要です' });
  }
  
  const errors = [];
  const successUsers = [];
  const waitingUsers = [];
  
  try {
    // CSVファイルを読み込み
    const csvData = await new Promise((resolve, reject) => {
      const data = [];
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (row) => {
          // CSVの最初の列をユーザー名として扱う
          const username = Object.values(row)[0];
          if (username && username.trim()) {
            data.push(username.replace('@', '').trim());
          }
        })
        .on('end', () => resolve(data))
        .on('error', reject);
    });
    
    // アップロードファイルを削除
    fs.unlinkSync(req.file.path);
    
    console.log(`CSV一括登録: ${csvData.length}件のユーザーを処理開始`);
    
    // 各ユーザーに対して処理
    for (const username of csvData) {
      try {
        // データベースで重複チェック
        const existingUser = await pool.query(
          'SELECT username FROM users WHERE username = $1', 
          [username]
        );
        
        if (existingUser.rows.length > 0) {
          errors.push(`${username}: 既に監視中です`);
          continue;
        }
        
        // 接続数制限チェック
        if (connections.size >= MAX_CONCURRENT_CONNECTIONS) {
          // データベースには追加するが、接続は待機
          await pool.query(`
            INSERT INTO users (username, status, is_live)
            VALUES ($1, 'waiting', false)
          `, [username]);
          
          connectionQueue.push(username);
          waitingUsers.push(username);
          
          console.log(`${username}: CSV経由で追加（待機キュー）`);
        } else {
          // TikTok接続を試行
          await connectToTikTokLive(username);
          successUsers.push(username);
          
          console.log(`${username}: CSV経由で追加成功`);
          
          // 即座ライブ状態チェック（非同期で実行）
          setTimeout(async () => {
            console.log(`${username}: CSV追加後の即座ライブ状態チェック`);
            await checkSingleUserLiveStatus(username);
          }, 15000 + (successUsers.length * 1000)); // 時間差を設けて負荷分散
        }
        
      } catch (error) {
        console.error(`${username}: CSV追加エラー - ${error.message}`);
        
        // エラーメッセージを分かりやすく
        let errorMessage = error.message;
        if (error.message.includes('LIVE has ended')) {
          errorMessage = '現在ライブ配信していません';
        } else if (error.message.includes('UserOfflineError')) {
          errorMessage = 'オフラインです';
        }
        
        errors.push(`${username}: ${errorMessage}`);
      }
    }
    
    // 結果を返す
    let responseMessage = `${successUsers.length}件のユーザーを追加しました`;
    if (waitingUsers.length > 0) {
      responseMessage += `（${waitingUsers.length}件は接続待機中）`;
    }
    
    console.log(`CSV一括登録完了: ${responseMessage}`);
    
    res.json({ 
      message: responseMessage,
      success: successUsers.length,
      waiting: waitingUsers.length,
      total: csvData.length,
      errors: errors.length > 0 ? errors : undefined
    });
    
  } catch (error) {
    console.error('CSV処理エラー:', error);
    
    // ファイルが残っている場合は削除
    if (fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'CSVファイルの処理中にエラーが発生しました: ' + error.message 
    });
  }
});

// 特定ユーザーの詳細チェック（テスト用）
app.post('/api/check-user-detailed', async (req, res) => {
  const { username } = req.body;
  
  if (!username) {
    return res.status(400).json({ error: 'ユーザー名が必要です' });
  }
  
  const cleanUsername = username.replace('@', '').trim();
  
  try {
    console.log(`${cleanUsername}: 詳細チェック開始`);
    
    // 第1段階：基本チェック
    const basicResult = await checkSingleUserLiveStatus(cleanUsername);
    
    // 第2段階：高精度チェック
    const accurateResult = await checkSingleUserLiveStatusAccurate(cleanUsername);
    
    // 現在のliveDataの状態
    const currentData = liveData.get(cleanUsername);
    
    res.json({
      username: cleanUsername,
      basicCheck: basicResult,
      accurateCheck: accurateResult,
      currentLiveData: currentData || null,
      finalStatus: accurateResult !== null ? accurateResult : basicResult,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error(`${cleanUsername}: 詳細チェックエラー`, error);
    res.status(500).json({ error: `チェックエラー: ${error.message}` });
  }
});

// 緊急対応：liveData手動復元API
app.post('/api/restore-live-data', async (req, res) => {
  try {
    console.log('手動liveData復元開始...');
    
    const result = await pool.query(`
      SELECT username, total_diamonds, total_gifts, total_comments, 
             is_live, viewer_count, last_live_check
      FROM users 
      WHERE status IN ('monitoring', 'waiting')
    `);
    
    let restoredCount = 0;
    
    for (const user of result.rows) {
      // 既存のliveDataがない場合のみ復元
      if (!liveData.has(user.username)) {
        const userData = {
          username: user.username,
          isLive: user.is_live || false,
          viewerCount: user.viewer_count || 0,
          totalComments: user.total_comments || 0,
          totalGifts: user.total_gifts || 0,
          totalDiamonds: user.total_diamonds || 0,
          lastUpdate: user.last_live_check || new Date().toISOString(),
          recentComments: [],
          recentGifts: []
        };
        
        liveData.set(user.username, userData);
        restoredCount++;
        
        console.log(`${user.username}: liveDataを手動復元`);
      }
    }
    
    console.log(`手動liveData復元完了: ${restoredCount}件`);
    
    res.json({
      success: true,
      message: `${restoredCount}件のliveDataを復元しました`,
      liveDataSize: liveData.size,
      connectionsSize: connections.size
    });
    
  } catch (error) {
    console.error('手動liveData復元エラー:', error);
    res.status(500).json({ error: '復元に失敗しました' });
  }
});

// デバッグ用：ユーザー追加ログAPI
app.post('/api/debug-add-user', async (req, res) => {
  const { username } = req.body;
  
  console.log('=== デバッグ：ユーザー追加開始 ===');
  console.log('リクエストボディ:', req.body);
  console.log('受信ユーザー名:', username);
  
  if (!username) {
    console.log('エラー: ユーザー名が空');
    return res.status(400).json({ 
      error: 'ユーザー名が必要です',
      debug: { receivedBody: req.body }
    });
  }
  
  const cleanUsername = username.replace('@', '').trim();
  console.log('クリーンアップ後:', cleanUsername);
  
  try {
    // 現在の状態確認
    const currentState = {
      liveDataSize: liveData.size,
      connectionsSize: connections.size,
      queueLength: connectionQueue.length,
      maxConnections: MAX_CONCURRENT_CONNECTIONS
    };
    
    console.log('現在の状態:', currentState);
    
    // データベース状態確認
    const existingUser = await pool.query('SELECT username, status FROM users WHERE username = $1', [cleanUsername]);
    console.log('データベース確認結果:', existingUser.rows);
    
    // liveData確認
    const existingLiveData = liveData.get(cleanUsername);
    console.log('既存liveData:', existingLiveData);
    
    if (existingUser.rows.length > 0) {
      return res.status(400).json({ 
        error: 'このユーザーは既に監視中です',
        debug: {
          existingUser: existingUser.rows[0],
          existingLiveData: existingLiveData,
          currentState: currentState
        }
      });
    }
    
    // 実際の追加テスト（データベースのみ）
    const insertQuery = `
      INSERT INTO users (username, status, is_live, total_diamonds, total_gifts, total_comments, viewer_count, last_live_check)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING *
    `;
    
    const insertResult = await pool.query(insertQuery, [
      cleanUsername,
      'monitoring',
      false,
      0,
      0,
      0,
      0,
      new Date()
    ]);
    
    console.log('データベース挿入結果:', insertResult.rows[0]);
    
    res.json({
      success: true,
      message: 'デバッグ追加テスト成功',
      debug: {
        cleanUsername: cleanUsername,
        insertedUser: insertResult.rows[0],
        currentState: currentState
      }
    });
    
  } catch (error) {
    console.error('デバッグ追加エラー:', error);
    res.status(500).json({ 
      error: error.message,
      debug: {
        cleanUsername: cleanUsername,
        errorStack: error.stack
      }
    });
  }
});

// デバッグ用API
app.get('/api/debug-status', (req, res) => {
  res.json({
    liveDataSize: liveData.size,
    connectionsSize: connections.size,
    connectionQueueLength: connectionQueue.length,
    liveDataKeys: Array.from(liveData.keys()),
    connectionKeys: Array.from(connections.keys()),
    queueContents: connectionQueue,
    sampleLiveData: liveData.size > 0 ? Object.fromEntries(Array.from(liveData.entries()).slice(0, 3)) : {}
  });
});

// 接続状況確認API
app.get('/api/connection-status', async (req, res) => {
  try {
    const totalUsers = await pool.query('SELECT COUNT(*) FROM users');
    const activeUsers = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'monitoring\'');
    const waitingUsers = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'waiting\'');
    
    res.json({
      totalUsers: parseInt(totalUsers.rows[0].count),
      activeUsers: parseInt(activeUsers.rows[0].count),
      waitingUsers: parseInt(waitingUsers.rows[0].count),
      activeConnections: connections.size,
      queueLength: connectionQueue.length,
      maxConnections: MAX_CONCURRENT_CONNECTIONS,
      availableSlots: MAX_CONCURRENT_CONNECTIONS - connections.size
    });
  } catch (error) {
    res.status(500).json({ error: '接続状況の取得に失敗しました' });
  }
});

// 高精度ライブ状態チェック（手動実行）
app.post('/api/check-live-status-accurate', async (req, res) => {
  try {
    console.log('手動高精度ライブ状態チェック開始');
    await checkLiveStatusAccurate();
    
    res.json({ 
      message: '高精度ライブ状態チェックを実行しました',
      timestamp: new Date().toISOString(),
      liveDataSize: liveData.size,
      connectionsSize: connections.size
    });
  } catch (error) {
    console.error('手動高精度ライブ状態チェックエラー:', error);
    res.status(500).json({ error: 'ライブ状態チェックに失敗しました' });
  }
});

// 手動ライブ状態チェック（既存）
app.post('/api/check-live-status', async (req, res) => {
  try {
    console.log('手動ライブ状態チェック開始');
    await checkLiveStatus();
    
    res.json({ 
      message: 'ライブ状態チェックを実行しました',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('手動ライブ状態チェックエラー:', error);
    res.status(500).json({ error: 'ライブ状態チェックに失敗しました' });
  }
});

// 監視ユーザー一覧取得
app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT username, added_at, status, total_diamonds, total_gifts, 
             total_comments, is_live, viewer_count, last_live_check
      FROM users 
      WHERE status IN ('monitoring', 'waiting')
      ORDER BY added_at ASC
    `);
    
    const users = result.rows.map(row => ({
      username: row.username,
      addedAt: row.added_at,
      status: row.status
    }));
    
    res.json({
      users: users,
      liveData: Object.fromEntries(liveData)
    });
  } catch (error) {
    console.error('ユーザー一覧取得エラー:', error);
    res.status(500).json({ error: 'ユーザー一覧の取得に失敗しました' });
  }
});

// ランキング取得
app.get('/api/ranking', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT username, total_diamonds, total_gifts, total_comments, 
             is_live, viewer_count, last_live_check
      FROM users 
      WHERE status = 'monitoring' AND total_diamonds > 0
      ORDER BY total_diamonds DESC
    `);
    
    const dailyRanking = result.rows.map((user, index) => ({
      rank: index + 1,
      username: user.username,
      totalDiamonds: user.total_diamonds,
      totalGifts: user.total_gifts,
      totalComments: user.total_comments,
      viewerCount: user.viewer_count || 0,
      isLive: user.is_live,
      estimatedEarnings: Math.round(user.total_diamonds * 0.005 * 100) / 100,
      lastUpdate: user.last_live_check
    }));
    
    res.json({
      ranking: dailyRanking,
      totalUsers: await pool.query('SELECT COUNT(*) FROM users WHERE status IN (\'monitoring\', \'waiting\')').then(r => parseInt(r.rows[0].count)),
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
    status: 'TikTok Live Monitor API with PostgreSQL + CSV + Connection Management',
    timestamp: new Date().toISOString(),
    version: '2.2.0'
  });
});

// ヘルスチェック
app.get('/health', async (req, res) => {
  try {
    // データベース接続チェック
    await pool.query('SELECT 1');
    
    const userCount = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'monitoring\'');
    const waitingCount = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'waiting\'');
    
    res.json({ 
      status: 'OK', 
      timestamp: new Date().toISOString(),
      database: 'connected',
      monitoredUsers: parseInt(userCount.rows[0].count),
      waitingUsers: parseInt(waitingCount.rows[0].count),
      activeConnections: connections.size,
      queueLength: connectionQueue.length,
      maxConnections: MAX_CONCURRENT_CONNECTIONS,
      availableSlots: MAX_CONCURRENT_CONNECTIONS - connections.size,
      features: ['postgresql', 'csv-upload', 'auto-restore', 'connection-management']
    });
  } catch (error) {
    res.status(500).json({
      status: 'ERROR',
      timestamp: new Date().toISOString(),
      database: 'disconnected',
      error: error.message
    });
  }
});

// 定期的なライブ状態チェック（高精度版、3分ごと）
setInterval(() => {
  checkLiveStatusAccurate();
}, 3 * 60 * 1000); // 精度向上のため3分に戻す

// 定期的な履歴保存（10分ごと）
setInterval(() => {
  saveBulkLiveHistory();
}, 10 * 60 * 1000);

// 段階的ライブ状態チェック（精度向上版）
async function checkLiveStatusAccurate() {
  console.log('=== 高精度ライブ状態チェック開始 ===');
  console.log(`liveData件数: ${liveData.size}`);
  
  if (liveData.size === 0) {
    console.log('⚠️ liveDataが空です');
    return;
  }
  
  for (const [username, userData] of liveData) {
    console.log(`${username}: 段階的チェック開始 (現在: ${userData.isLive ? 'ライブ中' : 'オフライン'})`);
    
    // 第1段階：簡易チェック
    let isLive = await checkSingleUserLiveStatus(username);
    
    if (isLive === null) {
      console.log(`${username}: 第1段階チェック失敗、第2段階へ`);
      
      // 第2段階：高精度チェック
      isLive = await checkSingleUserLiveStatusAccurate(username);
      
      if (isLive === null) {
        console.log(`${username}: 第2段階チェック失敗、現在の状態を維持`);
        continue;
      }
    }
    
    const previousStatus = userData.isLive;
    
    // 状態変更の処理
    if (isLive !== previousStatus) {
      console.log(`${username}: 状態変更検出 ${previousStatus ? 'ライブ中' : 'オフライン'} → ${isLive ? 'ライブ中' : 'オフライン'}`);
      
      userData.isLive = isLive;
      userData.lastUpdate = new Date().toISOString();
      liveData.set(username, userData);
      
      // データベースに保存
      await saveUserToDatabase(username, userData);
      
      // 通知送信
      if (isLive) {
        // ライブ開始
        io.emit('user-connected', { username, status: 'connected' });
        io.emit('live-status-change', { 
          username, 
          status: 'online',
          timestamp: userData.lastUpdate,
          message: `${username} がライブを開始しました`
        });
        
        // TikTok接続を再開
        if (!connections.has(username) && connections.size < MAX_CONCURRENT_CONNECTIONS) {
          try {
            await connectToTikTokLive(username);
            console.log(`${username}: TikTok接続再開成功`);
          } catch (error) {
            console.error(`${username}: TikTok接続再開失敗`, error);
          }
        }
      } else {
        // ライブ終了
        const existingConnection = connections.get(username);
        if (existingConnection) {
          existingConnection.disconnect();
          connections.delete(username);
        }
        
        io.emit('user-disconnected', { username });
        io.emit('live-status-change', { 
          username, 
          status: 'offline',
          timestamp: userData.lastUpdate,
          message: `${username} がライブを終了しました`
        });
        
        // 接続枠が空いたのでキューを処理
        setTimeout(() => {
          processConnectionQueue();
        }, 1000);
      }
      
      io.emit('live-data-update', { username, data: userData });
    } else {
      console.log(`${username}: 状態変更なし (${isLive ? 'ライブ中' : 'オフライン'})`);
    }
  }
  
  console.log('=== 高精度ライブ状態チェック完了 ===');
}

// 通知機能付きライブ状態チェック関数（既存を更新）
async function checkLiveStatus() {
  console.log('=== ライブ状態チェック開始 ===');
  console.log(`liveData件数: ${liveData.size}`);
  console.log(`connections件数: ${connections.size}`);
  
  if (liveData.size === 0) {
    console.log('⚠️ liveDataが空です - ユーザーが存在しません');
    console.log('=== ライブ状態チェック完了 ===');
    return;
  }
  
  // デバッグ用: 現在のユーザーを表示
  console.log('現在のユーザー:', Array.from(liveData.keys()));
  
  for (const [username, userData] of liveData) {
    console.log(`${username}: 状態チェック開始 (現在: ${userData.isLive ? 'ライブ中' : 'オフライン'})`);
    
    if (userData.isLive) {
      try {
        const testConnection = new WebcastPushConnection(username, {
          enableExtendedGiftInfo: false,
        });
        
        console.log(`${username}: 接続テスト開始...`);
        await testConnection.connect();
        console.log(`${username}: ライブ配信中を確認`);
        testConnection.disconnect();
        
        // まだライブ中の場合、最終更新時間を更新
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        await saveUserToDatabase(username, userData);
        
      } catch (error) {
        console.log(`${username}: 接続テストでエラー - ${error.message}`);
        
        if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
          console.log(`${username}: ライブ終了を検出、オフライン設定`);
          
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
          
          // データベースに保存
          await saveUserToDatabase(username, userData);
          
          // 既存の接続を切断
          const existingConnection = connections.get(username);
          if (existingConnection) {
            existingConnection.disconnect();
          }
          
          // ライブ終了通知を送信
          io.emit('user-disconnected', { username });
          io.emit('live-data-update', { username, data: userData });
          io.emit('live-status-change', { 
            username, 
            status: 'offline',
            timestamp: userData.lastUpdate,
            message: `${username} がライブを終了しました`
          });
          
        } else {
          console.log(`${username}: 予期しないエラー`, error.message);
        }
      }
    } else {
      console.log(`${username}: オフライン状態なので、ライブ開始チェックを実行`);
      // オフラインユーザーのライブ開始チェック
      try {
        const testConnection = new WebcastPushConnection(username, {
          enableExtendedGiftInfo: false,
        });
        
        console.log(`${username}: オフライン→オンライン チェック開始...`);
        await testConnection.connect();
        console.log(`${username}: ライブ開始を検出！オンライン設定`);
        testConnection.disconnect();
        
        // ライブ開始
        userData.isLive = true;
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        // 本格的なTikTok接続を再開
        try {
          await connectToTikTokLive(username);
          console.log(`${username}: TikTok接続再開成功`);
        } catch (reconnectError) {
          console.error(`${username}: TikTok接続再開失敗`, reconnectError);
        }
        
        // ライブ開始通知を送信
        io.emit('user-connected', { username, status: 'connected' });
        io.emit('live-data-update', { username, data: userData });
        io.emit('live-status-change', { 
          username, 
          status: 'online',
          timestamp: userData.lastUpdate,
          message: `${username} がライブを開始しました`
        });
        
      } catch (error) {
        console.log(`${username}: オフライン状態確認 - ${error.message}`);
        // まだオフラインのまま（正常）
        if (!error.message.includes('LIVE has ended') && !error.message.includes('UserOfflineError')) {
          console.log(`${username}: オフラインチェック中に予期しないエラー`, error.message);
        }
      }
    }
  }
  
  console.log('=== ライブ状態チェック完了 ===');
}

// 一括履歴保存
async function saveBulkLiveHistory() {
  console.log('=== 履歴データ保存開始 ===');
  
  for (const [username, userData] of liveData) {
    try {
      await saveLiveHistory(username, userData);
    } catch (error) {
      console.error(`${username}: 履歴保存エラー`, error);
    }
  }
  
  console.log('=== 履歴データ保存完了 ===');
}

// サーバー起動
const PORT = process.env.PORT || 10000;

// データベース初期化後にサーバー起動
initializeDatabase().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`=== TikTok Live Monitor Server (Connection Management) ===`);
    console.log(`Server running on port ${PORT}`);
    console.log(`Max connections: ${MAX_CONCURRENT_CONNECTIONS}`);
    console.log(`Timestamp: ${new Date().toISOString()}`);
    console.log(`Features: PostgreSQL, CSV Upload, Auto Restore, Connection Management`);
    console.log(`Health check: /health`);
    console.log(`API Base: /api`);
  });
}).catch(error => {
  console.error('サーバー起動エラー:', error);
  process.exit(1);
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
process.on('SIGTERM', async () => {
  console.log('サーバーを停止しています...');
  
  // 全ての接続を切断
  connections.forEach((connection, username) => {
    console.log(`${username} の接続を切断中...`);
    connection.disconnect();
  });
  
  // データベース接続を閉じる
  await pool.end();
  
  server.close(() => {
    console.log('サーバーが停止しました');
    process.exit(0);
  });
});
