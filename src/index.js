var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// --- Backend: Durable Object for the Chat Room ---
var ChatRoom = class {
  constructor(state, env) {
    this.state = state;
    this.sessions = [];
    this.lastTimestamps = new Map();
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    await this.handleSession(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleSession(ws) {
    ws.accept();
    const session = { ws, quit: false };
    this.sessions.push(session);

    const history = await this.state.storage.get("messages") || [];
    ws.send(JSON.stringify({ type: "history", messages: history }));

    await this.updateAndBroadcastStatus();

    ws.addEventListener("message", async (msg) => {
      try {
        if (session.quit) return;

        const data = JSON.parse(msg.data);

        if (data.type === "identity") {
          session.id = data.id;
          session.name = data.name || this.generateName();
          session.avatar = data.avatar || "🤖";
          session.isIdentified = true;
          ws.send(JSON.stringify({ type: "info", message: "欢迎你, " + session.name + "!" }));
          ws.send(JSON.stringify({ type: "identity", id: session.id, name: session.name, avatar: session.avatar }));
          this.broadcast({ type: "info", message: session.name + " 加入了聊天。" });
          await this.updateAndBroadcastStatus();
          return;
        }

        if (!session.isIdentified) {
          ws.send(JSON.stringify({ type: "error", message: "请先设置身份再发送消息！" }));
          return;
        }

        if (data.type === "typing") {
          this.broadcast({ type: "typing", name: session.name, id: session.id }, session.id);
          return;
        }

        if (data.type === "retract") {
            const { messageId } = data;
            const currentHistory = await this.state.storage.get("messages") || [];
            const messageIndex = currentHistory.findIndex(m => m.messageId === messageId);

            if (messageIndex > -1) {
                const messageToRetract = currentHistory[messageIndex];
                if (messageToRetract.id === session.id && (Date.now() - messageToRetract.timestamp < 120000)) {
                    currentHistory[messageIndex].text = "此消息已被撤回";
                    currentHistory[messageIndex].isRetracted = true;
                    await this.state.storage.put("messages", currentHistory);
                    this.broadcast({ type: "retract", messageId: messageId });
                } else {
                    ws.send(JSON.stringify({ type: "error", message: "无法撤回此消息（超时或权限不足）。" }));
                }
            }
            return;
        }

        if (data.type === "chat") {
          const now = Date.now();
          const last = this.lastTimestamps.get(ws) || 0;
          if (now - last < 500) {
            ws.send(JSON.stringify({ type: "error", message: "你说话太快了！" }));
            return;
          }
          this.lastTimestamps.set(ws, now);

          const today = new Date().toISOString().split("T")[0];
          let lastWriteDate = await this.state.storage.get("lastWriteDate") || today;
          let dailyWrites = await this.state.storage.get("dailyWrites") || 0;
          if (today !== lastWriteDate) {
            dailyWrites = 0;
            await this.state.storage.put("lastWriteDate", today);
          }
          if (dailyWrites >= 100000) {
            ws.send(JSON.stringify({ type: "error", message: "今日额度已聊完，明天再来吧！" }));
            return;
          }

          const message = {
            messageId: crypto.randomUUID(),
            id: session.id,
            name: session.name,
            avatar: session.avatar,
            text: data.text.toString(),
            timestamp: now,
            isRetracted: false,
          };

          const currentHistory = await this.state.storage.get("messages") || [];
          currentHistory.push(message);
          while (currentHistory.length > 100) {
            currentHistory.shift();
          }

          await this.state.storage.transaction(async (txn) => {
            await txn.put("messages", currentHistory);
            await txn.put("dailyWrites", dailyWrites + 1);
          });

          this.broadcast({ type: "message", ...message });
          await this.updateAndBroadcastStatus();
        }
      } catch (e) {
        // Ignore errors
      }
    });

    const closeOrErrorHandler = __name(() => {
      if (!session.quit) {
        session.quit = true;
        this.sessions = this.sessions.filter((s) => s !== session);
        this.lastTimestamps.delete(ws);
        if (session.isIdentified) {
          this.broadcast({ type: "info", message: session.name + " 离开了。" });
          this.updateAndBroadcastStatus();
        }
      }
    }, "closeOrErrorHandler");

    ws.addEventListener("close", closeOrErrorHandler);
    ws.addEventListener("error", closeOrErrorHandler);
  }

  broadcast(message, excludeId = null) {
    const preparedMessage = JSON.stringify(message);
    this.sessions = this.sessions.filter((session) => {
      if (session.id === excludeId) return true;
      if (!session.isIdentified && message.type !== 'status') return true;
      try {
        session.ws.send(preparedMessage);
        return true;
      } catch (err) {
        session.quit = true;
        return false;
      }
    });
  }

  async updateAndBroadcastStatus() {
    const today = new Date().toISOString().split("T")[0];
    let lastWriteDate = await this.state.storage.get("lastWriteDate") || today;
    let dailyWrites = await this.state.storage.get("dailyWrites") || 0;
    if (today !== lastWriteDate) {
      dailyWrites = 0;
    }
    const remaining = 100000 - dailyWrites;
    const onlineCount = this.sessions.filter((s) => s.isIdentified).length;
    const statusMessage = JSON.stringify({
      type: "status",
      online: onlineCount,
      remaining: remaining > 0 ? remaining : 0,
    });
    this.sessions.forEach((session) => {
      try {
        if (!session.quit) session.ws.send(statusMessage);
      } catch (err) {
        session.quit = true;
      }
    });
    this.sessions = this.sessions.filter((s) => !s.quit);
  }

  generateName() {
    const adjectives = ["神秘的", "快乐的", "沉思的", "勇敢的", "聪明的", "好奇的"];
    const nouns = ["访客", "旅人", "思想家", "探险家", "梦想家", "观察者"];
    const randomAdj = adjectives[Math.floor(Math.random() * adjectives.length)];
    const randomNoun = nouns[Math.floor(Math.random() * nouns.length)];
    const randomNum = Math.floor(Math.random() * 9000) + 1000;
    return randomAdj + randomNoun + "_" + randomNum;
  }
};
__name(ChatRoom, "ChatRoom");

var src_default = {
  async fetch(request, env) {
    try {
      if (request.headers.get("Upgrade") === "websocket") {
        const id = env.CHAT_ROOM.idFromName("global-chat-room");
        const stub = env.CHAT_ROOM.get(id);
        return stub.fetch(request);
      } else {
        return new Response(HTML, {
          headers: { "Content-Type": "text/html;charset=UTF-8" },
        });
      }
    } catch (e) {
      return new Response(e.message);
    }
  },
};

var HTML = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>聊天</title>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dompurify/dist/purify.min.js"></script>
    <style>
        :root { --theme-color: #007bff; --bg-color: #f0f2f5; --panel-bg: #fff; --text-color: #333; --border-color: #ddd; --retracted-color: #999; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; background-color: var(--bg-color); display: flex; height: 100vh; overflow: hidden; }
        
        .app-container { display: flex; width: 100%; height: 100%; transition: transform 0.3s ease-in-out; }
        .main-content { flex-grow: 1; display: flex; flex-direction: column; width: 100%; height: 100%; position: relative; z-index: 1; background-color: var(--bg-color); }
        
        .chat-header { padding: 10px 20px; border-bottom: 1px solid var(--border-color); font-size: 12px; color: #666; text-align: center; position: relative; flex-shrink: 0; background-color: var(--panel-bg); display: flex; justify-content: center; align-items: center; }
        .header-buttons { position: absolute; right: 15px; top: 50%; transform: translateY(-50%); display: flex; gap: 10px; }
        .toggle-button { background: none; border: 1px solid var(--border-color); border-radius: 50%; width: 30px; height: 30px; cursor: pointer; font-size: 18px; line-height: 28px; z-index: 10; display: flex; align-items: center; justify-content: center; }
        
        .messages { flex-grow: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; }
        .message { display: flex; margin-bottom: 15px; max-width: 80%; position: relative; }
        .message .avatar { font-size: 24px; width: 40px; height: 40px; line-height: 40px; text-align: center; border-radius: 50%; background-color: #e9e9eb; margin-right: 10px; flex-shrink: 0; user-select: none; }
        .message .content { display: flex; flex-direction: column; }
        .message .meta { font-size: 12px; color: #888; margin-bottom: 5px; }
        .message .text { background-color: #e9e9eb; padding: 10px 15px; border-radius: 18px; word-wrap: break-word; }
        .message .text p { margin: 0; }
        .message.mine { align-self: flex-end; flex-direction: row-reverse; }
        .message.mine .avatar { margin-right: 0; margin-left: 10px; }
        .message.mine .text { background-color: var(--theme-color); color: white; }
        .message.mine .meta { text-align: right; }
        .message.info, .message.error { align-self: center; text-align: center; color: #aaa; font-size: 12px; max-width: 100%; }
        .message.error { color: #ff4d4f; font-weight: bold; }
        .message.retracted .text { color: var(--retracted-color); font-style: italic; }
        
        .message .retract-btn { display: none; position: absolute; top: 50%; transform: translateY(-50%); left: -30px; cursor: pointer; font-size: 14px; color: #aaa; }
        .message.mine:hover .retract-btn { display: block; }
        .message.mine .retract-btn { left: auto; right: -30px; }

        .text.collapsible { max-height: 100px; overflow: hidden; position: relative; cursor: pointer; }
        .text.collapsible::after { content: '... 点击展开'; position: absolute; bottom: 0; right: 0; width: 100%; text-align: right; background: linear-gradient(to right, transparent, #e9e9eb 50%); padding-right: 15px; padding-left: 30px; box-sizing: border-box; color: #555; font-size: 12px; font-weight: bold; }
        .message.mine .text.collapsible::after { background: linear-gradient(to right, transparent, var(--theme-color) 50%); color: white; }
        .text.expanded { max-height: none; cursor: default; }
        .text.expanded::after { display: none; }

        .input-area { display: flex; flex-direction: column; padding: 15px; border-top: 1px solid var(--border-color); flex-shrink: 0; background-color: var(--panel-bg); }
        .input-row { display: flex; width: 100%; align-items: center; position: relative; }
        #message-input { flex-grow: 1; border: 1px solid #ccc; border-radius: 20px; padding: 10px 15px; font-size: 16px; outline: none; resize: none; max-height: 150px; overflow-y: auto; }
        .input-actions { display: flex; align-items: center; margin-left: 10px; }
        #emoji-toggle { font-size: 24px; cursor: pointer; background: none; border: none; padding: 0 5px; }
        #send-button { background-color: var(--theme-color); color: white; border: none; border-radius: 20px; padding: 10px 20px; cursor: pointer; font-size: 16px; }
        .typing-indicator { height: 20px; font-size: 12px; color: #888; padding: 5px 0 0; }
        
        .emoji-picker { position: absolute; bottom: 55px; right: 0; background: var(--panel-bg); border: 1px solid var(--border-color); border-radius: 8px; box-shadow: 0 -4px 12px rgba(0,0,0,0.1); display: grid; grid-template-columns: repeat(8, 1fr); gap: 5px; padding: 10px; z-index: 10; display: none; }
        .emoji-picker.visible { display: grid; }
        .emoji-picker span { cursor: pointer; font-size: 22px; text-align: center; padding: 5px; border-radius: 4px; }
        .emoji-picker span:hover { background-color: #f0f0f0; }

        .side-panel { position: fixed; top: 0; right: 0; width: 300px; height: 100%; background-color: #f8f9fa; z-index: 20; display: flex; flex-direction: column; box-shadow: -5px 0 15px rgba(0,0,0,0.1); transition: transform 0.3s ease-in-out; transform: translateX(100%); padding: 0; box-sizing: border-box; }
        .app-container.settings-open .side-panel { transform: translateX(0); }
        .side-panel h3 { background-color: #e9ecef; color: #333; margin: 0; padding: 15px; text-align: center; font-size: 16px; flex-shrink: 0; }
        .settings-content { padding: 20px; overflow-y: auto; flex-grow: 1; }
        .setting-item { margin-bottom: 20px; }
        .setting-item label { display: block; margin-bottom: 5px; font-weight: bold; font-size: 14px; }
        .setting-item input[type="text"], .setting-item input[type="number"] { width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
        .setting-item .switch { position: relative; display: inline-block; width: 50px; height: 24px; }
        .setting-item .switch input { opacity: 0; width: 0; height: 0; }
        .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #ccc; transition: .4s; border-radius: 24px; }
        .slider:before { position: absolute; content: ""; height: 18px; width: 18px; left: 3px; bottom: 3px; background-color: white; transition: .4s; border-radius: 50%; }
        input:checked + .slider { background-color: var(--theme-color); }
        input:checked + .slider:before { transform: translateX(26px); }

        .avatar-selector { display: grid; grid-template-columns: repeat(auto-fill, minmax(40px, 1fr)); gap: 10px; }
        .avatar-option { font-size: 24px; text-align: center; padding: 5px; border-radius: 50%; cursor: pointer; transition: background-color 0.2s; }
        .avatar-option.selected { background-color: var(--theme-color); color: white; }
        #save-settings { background-color: var(--theme-color); color: white; border: none; border-radius: 4px; padding: 10px; width: 100%; cursor: pointer; font-size: 16px; margin-top: auto; }
        #clear-messages { background-color: #6c757d; color: white; border: none; border-radius: 4px; padding: 10px; width: 100%; cursor: pointer; font-size: 16px; margin-top: 10px; }

        @media (max-width: 768px) {
            .message { max-width: 90%; }
            .chat-header { padding: 10px 15px; }
            .side-panel { width: 280px; }
            .app-container.settings-open .main-content { transform: translateX(-280px); }
        }
    </style>
</head>
<body>
    <div class="app-container" id="app-container">
        <div class="main-content">
            <div class="chat-header">
                <span id="status">正在连接...</span>
                <div class="header-buttons">
                    <button id="toggle-settings" class="toggle-button" title="设置">⚙️</button>
                </div>
            </div>
            <div class="messages" id="messages"></div>
            <div class="input-area">
                <div class="typing-indicator" id="typing-indicator"></div>
                <div class="input-row">
                   <textarea id="message-input" placeholder="输入消息..." autocomplete="off" rows="1"></textarea>
                   <div class="input-actions">
                       <button id="emoji-toggle" title="表情">😀</button>
                       <button id="send-button" title="发送">发送</button>
                   </div>
                   <div class="emoji-picker" id="emoji-picker"></div>
                </div>
            </div>
        </div>
        <div class="side-panel settings-panel">
            <h3>个人与显示设置</h3>
            <div class="settings-content">
                <div class="setting-item">
                    <label for="name-input">昵称</label>
                    <input type="text" id="name-input" placeholder="设置你的昵称">
                </div>
                <div class="setting-item">
                    <label>头像</label>
                    <div class="avatar-selector" id="avatar-selector"></div>
                </div>
                <hr>
                <div class="setting-item">
                    <label for="render-html-toggle">渲染富文本 (HTML/Markdown)</label>
                    <label class="switch">
                        <input type="checkbox" id="render-html-toggle">
                        <span class="slider"></span>
                    </label>
                </div>
                <div class="setting-item">
                    <label for="history-limit-input">加载历史消息数量</label>
                    <input type="number" id="history-limit-input" min="1" max="100" value="50">
                </div>
                <div class="setting-item">
                    <button id="clear-messages">清空当前聊天记录</button>
                </div>
            </div>
            <div style="padding: 20px; border-top: 1px solid var(--border-color);">
                <button id="save-settings">保存并加入聊天</button>
            </div>
        </div>
    </div>

    <script>
    (function() {
        // --- UI Elements ---
        const ui = {
            appContainer: document.getElementById('app-container'),
            messagesDiv: document.getElementById('messages'),
            statusSpan: document.getElementById('status'),
            input: document.getElementById('message-input'),
            sendButton: document.getElementById('send-button'),
            nameInput: document.getElementById('name-input'),
            avatarSelector: document.getElementById('avatar-selector'),
            saveButton: document.getElementById('save-settings'),
            toggleSettingsButton: document.getElementById('toggle-settings'),
            emojiToggleButton: document.getElementById('emoji-toggle'),
            emojiPicker: document.getElementById('emoji-picker'),
            typingIndicator: document.getElementById('typing-indicator'),
            renderHtmlToggle: document.getElementById('render-html-toggle'),
            historyLimitInput: document.getElementById('history-limit-input'),
            clearMessagesButton: document.getElementById('clear-messages'),
        };

        // --- State ---
        let socket;
        let myIdentity = { id: '', name: '', avatar: '' };
        let clientSettings = { renderHtml: false, historyLimit: 50 };
        const avatars = ['😀', '😎', '🤖', '👻', '👽', '🧑‍🚀', '🦄', '🐼', '🦊', '🧙'];
        const emojis = ['👍', '❤️', '😂', '😮', '😢', '🙏', '🎉', '🔥', '💯', '🤔', '😊', '🥳', '🤯', '🤣', '🙌', '✨'];
        let typingTimeout;
        const typingUsers = new Map();
        let reconnectAttempts = 0;
        let fullHistory = [];

        // --- Initialization ---
        function initialize() {
            loadIdentity();
            loadClientSettings();
            populateAvatars();
            populateEmojis();
            connect();
            setupEventListeners();
            autoResizeTextarea();
        }

        function setupEventListeners() {
            ui.sendButton.addEventListener('click', sendMessage);
            ui.input.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault();
                    sendMessage();
                } else {
                    sendTyping();
                }
            });
            ui.saveButton.addEventListener('click', saveAndIdentify);
            ui.toggleSettingsButton.addEventListener('click', () => ui.appContainer.classList.toggle('settings-open'));
            ui.emojiToggleButton.addEventListener('click', (e) => {
                e.stopPropagation();
                ui.emojiPicker.classList.toggle('visible');
            });
            document.addEventListener('click', (e) => {
                if (ui.emojiPicker && !ui.emojiPicker.contains(e.target) && e.target !== ui.emojiToggleButton) {
                    ui.emojiPicker.classList.remove('visible');
                }
            });
            ui.clearMessagesButton.addEventListener('click', () => {
                ui.messagesDiv.innerHTML = '';
                addSystemMessage('屏幕已清空', 'info');
            });
            ui.messagesDiv.addEventListener('click', handleMessageClick);
        }

        // --- Settings & Identity ---
        function loadIdentity() {
            myIdentity.id = localStorage.getItem('chat_id') || crypto.randomUUID();
            localStorage.setItem('chat_id', myIdentity.id);
            myIdentity.name = localStorage.getItem('chat_name') || '';
            myIdentity.avatar = localStorage.getItem('chat_avatar') || avatars[0];
            ui.nameInput.value = myIdentity.name;
        }

        function loadClientSettings() {
            const savedSettings = JSON.parse(localStorage.getItem('chat_client_settings'));
            if (savedSettings) {
                clientSettings.renderHtml = savedSettings.renderHtml === true;
                clientSettings.historyLimit = parseInt(savedSettings.historyLimit, 10) || 50;
            }
            ui.renderHtmlToggle.checked = clientSettings.renderHtml;
            ui.historyLimitInput.value = clientSettings.historyLimit;
        }

        function saveClientSettings() {
            const oldSettings = { ...clientSettings };
            clientSettings.renderHtml = ui.renderHtmlToggle.checked;
            clientSettings.historyLimit = parseInt(ui.historyLimitInput.value, 10) || 50;
            localStorage.setItem('chat_client_settings', JSON.stringify(clientSettings));
            
            // 如果设置有变，重新渲染消息
            if (oldSettings.renderHtml !== clientSettings.renderHtml || oldSettings.historyLimit !== clientSettings.historyLimit) {
                renderHistory();
            }
        }

        function populateAvatars() {
            ui.avatarSelector.innerHTML = '';
            avatars.forEach(avatar => {
                const option = document.createElement('div');
                option.classList.add('avatar-option');
                option.textContent = avatar;
                if (avatar === myIdentity.avatar) option.classList.add('selected');
                option.addEventListener('click', (e) => {
                    ui.avatarSelector.querySelector('.selected')?.classList.remove('selected');
                    e.currentTarget.classList.add('selected');
                });
                ui.avatarSelector.appendChild(option);
            });
        }

        function populateEmojis() {
            emojis.forEach(emoji => {
                const span = document.createElement('span');
                span.textContent = emoji;
                span.addEventListener('click', () => {
                    ui.input.value += emoji;
                    ui.input.focus();
                    autoResizeTextarea();
                });
                ui.emojiPicker.appendChild(span);
            });
        }

        function saveAndIdentify() {
            myIdentity.name = ui.nameInput.value.trim();
            myIdentity.avatar = ui.avatarSelector.querySelector('.selected').textContent;
            localStorage.setItem('chat_name', myIdentity.name);
            localStorage.setItem('chat_avatar', myIdentity.avatar);
            
            saveClientSettings();
            
            if (socket && socket.readyState === WebSocket.OPEN) {
                sendIdentity();
            }
            ui.appContainer.classList.remove('settings-open');
            addSystemMessage('设置已保存。', 'info');
        }

        // --- WebSocket Logic ---
        function connect() {
            const protocol = window.location.protocol === "https:" ? "wss" : "ws";
            const wsUrl = protocol + '://' + window.location.host + '/';
            socket = new WebSocket(wsUrl);
            socket.onopen = onSocketOpen;
            socket.onmessage = onSocketMessage;
            socket.onclose = onSocketClose;
            socket.onerror = onSocketError;
        }

        function onSocketOpen() {
            reconnectAttempts = 0;
            sendIdentity();
        }
        
        function sendIdentity() {
            if (socket && socket.readyState === WebSocket.OPEN && myIdentity.name) {
                socket.send(JSON.stringify({ type: 'identity', ...myIdentity }));
            }
        }

        function onSocketMessage(event) {
            try {
                const data = JSON.parse(event.data);
                switch (data.type) {
                    case 'info':
                    case 'error':
                        addSystemMessage(data.message, data.type);
                        break;
                    case 'status':
                        updateStatus(data.online, data.remaining);
                        break;
                    case 'history':
                        fullHistory = data.messages;
                        renderHistory();
                        break;
                    case 'message':
                        fullHistory.push(data);
                        if (fullHistory.length > 100) fullHistory.shift();
                        addMessage(data);
                        break;
                    case 'identity':
                        myIdentity = { id: data.id, name: data.name, avatar: data.avatar };
                        break;
                    case 'typing':
                        if (data.id !== myIdentity.id) {
                            updateTypingIndicator(data.name, true);
                        }
                        break;
                    case 'retract':
                        const msgIndex = fullHistory.findIndex(m => m.messageId === data.messageId);
                        if (msgIndex > -1) {
                            fullHistory[msgIndex].text = "此消息已被撤回";
                            fullHistory[msgIndex].isRetracted = true;
                        }
                        retractMessageOnScreen(data.messageId);
                        break;
                }
                scrollToBottom();
            } catch (e) {
                console.error('解析消息时出错:', e);
            }
        }

        function onSocketClose() {
            reconnectAttempts++;
            const delay = Math.min(30000, (Math.pow(2, reconnectAttempts) * 1000));
            const jitter = delay * 0.2 * Math.random();
            const reconnectDelay = delay + jitter;
            addSystemMessage('连接已断开，将在 ' + Math.round(reconnectDelay / 1000) + ' 秒后尝试重连...', 'error');
            setTimeout(connect, reconnectDelay);
        }

        function onSocketError(error) {
            console.error('WebSocket 错误:', error);
            addSystemMessage('连接发生错误。', 'error');
        }

        // --- UI Rendering & Actions ---
        function renderHistory() {
            ui.messagesDiv.innerHTML = '';
            const limitedHistory = fullHistory.slice(-clientSettings.historyLimit);
            limitedHistory.forEach(msg => addMessage(msg));
        }

        function addMessage(msg) {
            const isMine = msg.id === myIdentity.id;
            const msgEl = document.createElement('div');
            msgEl.className = 'message ' + (isMine ? 'mine' : 'theirs');
            msgEl.dataset.messageId = msg.messageId;

            let textContent;
            if (msg.isRetracted) {
                msgEl.classList.add('retracted');
                textContent = escapeHtml(msg.text);
            } else if (clientSettings.renderHtml) {
                textContent = DOMPurify.sanitize(marked.parse(msg.text));
            } else {
                textContent = escapeHtml(msg.text).replace(/\\n/g, '<br>');
            }
            
            const textDiv = document.createElement('div');
            textDiv.className = 'text';
            textDiv.innerHTML = textContent;

            if (!msg.isRetracted && msg.text.length > 200) {
                textDiv.classList.add('collapsible');
            }

            const canRetract = isMine && !msg.isRetracted && (Date.now() - msg.timestamp < 120000);

            const metaHTML = '<div class="meta">' + escapeHtml(msg.name) + ' - ' + new Date(msg.timestamp).toLocaleTimeString() + '</div>';
            const avatarHTML = '<div class="avatar">' + escapeHtml(msg.avatar) + '</div>';
            const retractBtnHTML = canRetract ? '<span class="retract-btn" title="撤回">↶</span>' : '';

            const contentDiv = document.createElement('div');
            contentDiv.className = 'content';
            contentDiv.innerHTML = metaHTML;
            contentDiv.appendChild(textDiv);

            msgEl.innerHTML = avatarHTML;
            msgEl.appendChild(contentDiv);
            msgEl.innerHTML += retractBtnHTML;
            
            ui.messagesDiv.appendChild(msgEl);
        }

        function retractMessageOnScreen(messageId) {
            const msgEl = document.querySelector('[data-message-id="' + messageId + '"]');
            if (msgEl) {
                const textEl = msgEl.querySelector('.text');
                textEl.innerHTML = '此消息已被撤回';
                textEl.className = 'text';
                msgEl.classList.add('retracted');
                const retractBtn = msgEl.querySelector('.retract-btn');
                if (retractBtn) retractBtn.remove();
            }
        }

        function addSystemMessage(text, type) {
            const msgEl = document.createElement('div');
            msgEl.className = 'message ' + type;
            msgEl.textContent = escapeHtml(text);
            ui.messagesDiv.appendChild(msgEl);
        }

        function updateStatus(online, remaining) {
            ui.statusSpan.textContent = '在线: ' + online + ' 人 | 今日剩余消息: ' + remaining.toLocaleString();
        }

        function sendMessage() {
            const text = ui.input.value.trim();
            if (socket && socket.readyState === WebSocket.OPEN && text !== '') {
                if (!myIdentity.name) {
                    addSystemMessage('请先在设置中设置昵称和头像！', 'error');
                    ui.appContainer.classList.add('settings-open');
                    return;
                }
                socket.send(JSON.stringify({ type: 'chat', text: text }));
                ui.input.value = '';
                autoResizeTextarea();
            }
        }

        function sendTyping() {
            if (socket && socket.readyState === WebSocket.OPEN && !typingTimeout) {
                socket.send(JSON.stringify({ type: 'typing' }));
                typingTimeout = setTimeout(() => { typingTimeout = null; }, 2000);
            }
        }

        function updateTypingIndicator(name, isTyping) {
            if (isTyping) {
                typingUsers.set(name, Date.now());
            } else {
                typingUsers.delete(name);
            }

            const now = Date.now();
            for (const [userName, lastTyped] of typingUsers.entries()) {
                if (now - lastTyped > 3000) {
                    typingUsers.delete(userName);
                }
            }

            const names = Array.from(typingUsers.keys());
            if (names.length === 0) {
                ui.typingIndicator.textContent = '';
            } else if (names.length === 1) {
                ui.typingIndicator.textContent = names[0] + ' 正在输入...';
            } else if (names.length === 2) {
                ui.typingIndicator.textContent = names.join(' 和 ') + ' 正在输入...';
            } else {
                ui.typingIndicator.textContent = '多个人正在输入...';
            }
        }
        
        function handleMessageClick(event) {
            const textEl = event.target.closest('.text.collapsible');
            if (textEl) {
                textEl.classList.remove('collapsible');
                textEl.classList.add('expanded');
                return;
            }
            const retractBtn = event.target.closest('.retract-btn');
            if (retractBtn) {
                const messageEl = retractBtn.closest('.message');
                const messageId = messageEl.dataset.messageId;
                if (confirm('确定要撤回这条消息吗？')) {
                    socket.send(JSON.stringify({ type: 'retract', messageId: messageId }));
                }
            }
        }

        // --- Utilities ---
        function escapeHtml(unsafe) {
            if (typeof unsafe !== 'string') return '';
            return unsafe.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
        }

        function scrollToBottom() {
            // Only scroll if user is near the bottom
            const shouldScroll = ui.messagesDiv.scrollTop + ui.messagesDiv.clientHeight >= ui.messagesDiv.scrollHeight - 100;
            if(shouldScroll) {
                ui.messagesDiv.scrollTop = ui.messagesDiv.scrollHeight;
            }
        }
        
        function autoResizeTextarea() {
            ui.input.style.height = 'auto';
            const scrollHeight = ui.input.scrollHeight;
            ui.input.style.height = scrollHeight + 'px';
        }
        ui.input.addEventListener('input', autoResizeTextarea);

        // --- Start the application ---
        document.addEventListener('DOMContentLoaded', initialize);
    })();
    </script>
</body>
</html>
`;
export {
  ChatRoom,
  src_default as default
};
//# sourceMappingURL=index.js.map
