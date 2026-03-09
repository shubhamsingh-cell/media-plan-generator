/**
 * Nova by Joveo Chat Widget
 *
 * Self-contained chat interface for Nova by Joveo recruitment marketing intelligence.
 * Drop-in widget that can be embedded in any page.
 *
 * Usage:
 *   <script src="/static/nova-chat.js"></script>
 *   <div id="nova-chat"></div>
 *   <script>NovaChat.init({ containerId: 'nova-chat' });</script>
 *
 * Or use as floating widget (no container needed):
 *   <script src="/static/nova-chat.js"></script>
 *   <script>NovaChat.init();</script>
 */
(function () {
  'use strict';

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------
  var CONFIG = {
    apiUrl: '/api/chat',
    primaryColor: '#0A66C9',
    primaryDark: '#191919',
    primaryLight: '#D1E8FF',
    accentColor: '#0A66C9',
    accentLight: '#4A90D9',
    textColor: '#1A1A1A',
    textLight: '#6B6B6B',
    bgColor: '#FFFFFF',
    bgLight: '#F9FAFB',
    borderColor: '#E5E7EB',
    errorColor: '#DC2626',
    successColor: '#059669',
    maxHistoryStorage: 50,
    storageKey: 'nova_chat_history',
    sessionKey: 'nova_session',
    widgetWidth: '400px',
    widgetHeight: '580px',
    mobileBreakpoint: 640,
  };

  var SUGGESTED_QUESTIONS = [
    'What publishers work best for nursing roles?',
    'Compare CPC benchmarks across Google, LinkedIn, and Indeed for healthcare',
    "How difficult is it to hire software engineers right now?",
    'Recommend a $50K budget allocation for 10 engineering hires',
  ];

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  var state = {
    isOpen: false,
    isLoading: false,
    messages: [],
    sessionId: null,
    container: null,
    chatPanel: null,
    floatingBtn: null,
    initialized: false,
  };

  // ---------------------------------------------------------------------------
  // Styles (injected once)
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById('nova-styles')) return;

    var css = ''
      + '#nova-float-btn {'
      + '  position: fixed; bottom: 24px; right: 24px; z-index: 99999;'
      + '  width: 56px; height: 56px; border-radius: 16px;'
      + '  background: linear-gradient(135deg, ' + CONFIG.primaryDark + ' 0%, ' + CONFIG.primaryColor + ' 100%);'
      + '  color: #fff; border: none; cursor: pointer;'
      + '  box-shadow: 0 4px 20px rgba(25,25,25,0.35), 0 0 0 1px rgba(255,255,255,0.1) inset;'
      + '  display: flex; align-items: center; justify-content: center;'
      + '  transition: transform 0.25s cubic-bezier(0.4, 0, 0.2, 1), box-shadow 0.25s ease;'
      + '  font-size: 0;'
      + '}'
      + '#nova-float-btn:hover {'
      + '  transform: translateY(-2px) scale(1.05);'
      + '  box-shadow: 0 8px 28px rgba(25,25,25,0.45), 0 0 0 1px rgba(255,255,255,0.15) inset;'
      + '}'
      + '#nova-float-btn:active {'
      + '  transform: scale(0.97);'
      + '}'
      + '#nova-float-btn svg { width: 26px; height: 26px; }'

      + '#nova-panel {'
      + '  position: fixed; bottom: 92px; right: 24px; z-index: 99998;'
      + '  width: ' + CONFIG.widgetWidth + '; height: ' + CONFIG.widgetHeight + ';'
      + '  max-height: calc(100vh - 120px);'
      + '  background: ' + CONFIG.bgColor + ';'
      + '  border-radius: 20px;'
      + '  box-shadow: 0 12px 48px rgba(25,25,25,0.18), 0 2px 8px rgba(0,0,0,0.06);'
      + '  display: flex; flex-direction: column;'
      + '  overflow: hidden;'
      + '  transition: opacity 0.3s cubic-bezier(0.4, 0, 0.2, 1), transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);'
      + '  font-family: "Calibri", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;'
      + '  border: 1px solid rgba(0,0,0,0.06);'
      + '}'
      + '#nova-panel.nova-hidden {'
      + '  opacity: 0; transform: translateY(16px) scale(0.95); pointer-events: none;'
      + '}'
      + '#nova-panel.nova-visible {'
      + '  opacity: 1; transform: translateY(0) scale(1); pointer-events: auto;'
      + '}'

      // Header
      + '.nova-header {'
      + '  background: linear-gradient(135deg, ' + CONFIG.primaryDark + ' 0%, ' + CONFIG.primaryColor + ' 100%);'
      + '  color: #fff; padding: 18px 20px;'
      + '  display: flex; align-items: center; justify-content: space-between;'
      + '  flex-shrink: 0;'
      + '  position: relative;'
      + '  overflow: hidden;'
      + '}'
      + '.nova-header::after {'
      + '  content: "";'
      + '  position: absolute; bottom: 0; left: 0; right: 0; height: 1px;'
      + '  background: linear-gradient(90deg, transparent, rgba(34,211,238,0.4), transparent);'
      + '}'
      + '.nova-header-left {'
      + '  display: flex; align-items: center; gap: 10px;'
      + '}'
      + '.nova-header-icon {'
      + '  width: 36px; height: 36px; border-radius: 10px;'
      + '  background: rgba(255,255,255,0.15);'
      + '  backdrop-filter: blur(8px);'
      + '  border: 1px solid rgba(255,255,255,0.2);'
      + '  display: flex; align-items: center; justify-content: center;'
      + '  font-size: 18px;'
      + '  box-shadow: 0 2px 8px rgba(0,0,0,0.1);'
      + '}'
      + '.nova-header-title {'
      + '  font-size: 16px; font-weight: 700; letter-spacing: 0.3px;'
      + '}'
      + '.nova-header-subtitle {'
      + '  font-size: 11px; opacity: 0.85; margin-top: 1px;'
      + '}'
      + '.nova-close-btn {'
      + '  background: none; border: none; color: #fff; cursor: pointer;'
      + '  padding: 4px; border-radius: 6px; line-height: 1;'
      + '  transition: background 0.15s;'
      + '}'
      + '.nova-close-btn:hover { background: rgba(255,255,255,0.2); }'
      + '.nova-close-btn svg { width: 20px; height: 20px; }'

      // Messages area
      + '.nova-messages {'
      + '  flex: 1; overflow-y: auto; padding: 16px;'
      + '  display: flex; flex-direction: column; gap: 12px;'
      + '  background: ' + CONFIG.bgLight + ';'
      + '}'
      + '.nova-messages::-webkit-scrollbar { width: 5px; }'
      + '.nova-messages::-webkit-scrollbar-track { background: transparent; }'
      + '.nova-messages::-webkit-scrollbar-thumb { background: ' + CONFIG.borderColor + '; border-radius: 4px; }'

      // Message bubbles
      + '.nova-msg {'
      + '  max-width: 85%; padding: 11px 15px; border-radius: 14px;'
      + '  font-size: 13.5px; line-height: 1.6; word-wrap: break-word;'
      + '  animation: nova-msgIn 0.25s ease-out;'
      + '}'
      + '@keyframes nova-msgIn {'
      + '  from { opacity: 0; transform: translateY(8px); }'
      + '  to { opacity: 1; transform: translateY(0); }'
      + '}'
      + '.nova-msg-user {'
      + '  align-self: flex-end;'
      + '  background: linear-gradient(135deg, ' + CONFIG.primaryDark + ', ' + CONFIG.primaryColor + ');'
      + '  color: #fff;'
      + '  border-bottom-right-radius: 4px;'
      + '  box-shadow: 0 1px 4px rgba(25,25,25,0.15);'
      + '}'
      + '.nova-msg-assistant {'
      + '  align-self: flex-start;'
      + '  background: ' + CONFIG.bgColor + '; color: ' + CONFIG.textColor + ';'
      + '  border: 1px solid ' + CONFIG.borderColor + ';'
      + '  border-bottom-left-radius: 4px;'
      + '  box-shadow: 0 1px 3px rgba(0,0,0,0.04);'
      + '}'

      // Markdown inside messages
      + '.nova-msg-assistant h3 {'
      + '  font-size: 14px; font-weight: 700; margin: 8px 0 4px 0;'
      + '  color: ' + CONFIG.primaryColor + ';'
      + '}'
      + '.nova-msg-assistant h3:first-child { margin-top: 0; }'
      + '.nova-msg-assistant strong { font-weight: 600; }'
      + '.nova-msg-assistant em { font-style: italic; color: ' + CONFIG.textLight + '; }'
      + '.nova-msg-assistant ul, .nova-msg-assistant ol {'
      + '  margin: 4px 0; padding-left: 18px;'
      + '}'
      + '.nova-msg-assistant li { margin-bottom: 2px; }'
      + '.nova-msg-assistant table {'
      + '  border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 12px;'
      + '}'
      + '.nova-msg-assistant th, .nova-msg-assistant td {'
      + '  border: 1px solid ' + CONFIG.borderColor + '; padding: 4px 8px; text-align: left;'
      + '}'
      + '.nova-msg-assistant th {'
      + '  background: ' + CONFIG.primaryLight + '; font-weight: 600;'
      + '}'
      + '.nova-msg-assistant code {'
      + '  background: ' + CONFIG.bgLight + '; padding: 1px 4px; border-radius: 3px;'
      + '  font-family: "SF Mono", Monaco, Menlo, monospace; font-size: 12px;'
      + '}'
      + '.nova-msg-assistant p {'
      + '  margin: 4px 0;'
      + '}'

      // Meta info (sources, confidence)
      + '.nova-msg-meta {'
      + '  margin-top: 8px; padding-top: 6px;'
      + '  border-top: 1px solid ' + CONFIG.borderColor + ';'
      + '  display: flex; flex-wrap: wrap; gap: 4px; align-items: center;'
      + '}'
      + '.nova-badge {'
      + '  font-size: 10px; padding: 2px 6px; border-radius: 10px;'
      + '  background: ' + CONFIG.primaryLight + '; color: ' + CONFIG.primaryColor + ';'
      + '  font-weight: 500; white-space: nowrap;'
      + '}'
      + '.nova-confidence {'
      + '  font-size: 10px; padding: 2px 6px; border-radius: 10px;'
      + '  font-weight: 600; white-space: nowrap; cursor: pointer;'
      + '  position: relative;'
      + '}'
      + '.nova-confidence-high { background: #D1FAE5; color: #065F46; }'
      + '.nova-confidence-medium { background: #FEF3C7; color: #92400E; }'
      + '.nova-confidence-low { background: #FEE2E2; color: #991B1B; }'
      + '.nova-confidence-tooltip {'
      + '  display: none; position: absolute; bottom: 100%; left: 50%;'
      + '  transform: translateX(-50%); margin-bottom: 6px;'
      + '  background: #1B2A4A; color: #fff; padding: 10px 14px;'
      + '  border-radius: 8px; font-size: 11px; line-height: 1.5;'
      + '  min-width: 260px; max-width: 340px; z-index: 999;'
      + '  box-shadow: 0 4px 12px rgba(0,0,0,0.15); font-weight: 400;'
      + '  white-space: normal; text-align: left;'
      + '}'
      + '.nova-confidence:hover .nova-confidence-tooltip { display: block; }'
      + '.nova-tooltip-title { font-weight: 600; margin-bottom: 4px; font-size: 12px; }'
      + '.nova-tooltip-row { display: flex; justify-content: space-between; padding: 1px 0; }'
      + '.nova-tooltip-divider { border-top: 1px solid rgba(255,255,255,0.2); margin: 4px 0; }'
      + '.nova-tooltip-note { font-size: 9px; opacity: 0.7; margin-top: 4px; }'

      // Typing indicator
      + '.nova-typing {'
      + '  align-self: flex-start; display: flex; gap: 4px;'
      + '  padding: 12px 16px; background: ' + CONFIG.bgColor + ';'
      + '  border: 1px solid ' + CONFIG.borderColor + ';'
      + '  border-radius: 12px; border-bottom-left-radius: 4px;'
      + '}'
      + '.nova-typing-dot {'
      + '  width: 7px; height: 7px; border-radius: 50%;'
      + '  background: ' + CONFIG.textLight + ';'
      + '  animation: nova-bounce 1.4s infinite;'
      + '}'
      + '.nova-typing-dot:nth-child(2) { animation-delay: 0.2s; }'
      + '.nova-typing-dot:nth-child(3) { animation-delay: 0.4s; }'
      + '@keyframes nova-bounce {'
      + '  0%, 60%, 100% { transform: translateY(0); }'
      + '  30% { transform: translateY(-6px); }'
      + '}'

      // Suggested questions
      + '.nova-suggestions {'
      + '  padding: 12px 16px 8px; display: flex; flex-direction: column; gap: 6px;'
      + '}'
      + '.nova-suggestions-title {'
      + '  font-size: 11px; color: ' + CONFIG.textLight + '; font-weight: 600;'
      + '  text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;'
      + '}'
      + '.nova-suggestion-btn {'
      + '  background: ' + CONFIG.bgColor + '; border: 1px solid ' + CONFIG.borderColor + ';'
      + '  padding: 9px 14px; border-radius: 10px; cursor: pointer;'
      + '  font-size: 12.5px; color: ' + CONFIG.textColor + '; text-align: left;'
      + '  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1); line-height: 1.4;'
      + '}'
      + '.nova-suggestion-btn:hover {'
      + '  border-color: ' + CONFIG.accentColor + ';'
      + '  background: rgba(10,102,201,0.05);'
      + '  transform: translateX(3px);'
      + '  box-shadow: 0 1px 4px rgba(10,102,201,0.1);'
      + '}'

      // Input area
      + '.nova-input-area {'
      + '  padding: 12px 16px; border-top: 1px solid ' + CONFIG.borderColor + ';'
      + '  display: flex; gap: 8px; align-items: flex-end;'
      + '  background: ' + CONFIG.bgColor + '; flex-shrink: 0;'
      + '}'
      + '.nova-input {'
      + '  flex: 1; border: 1px solid ' + CONFIG.borderColor + ';'
      + '  border-radius: 10px; padding: 10px 14px;'
      + '  font-size: 13.5px; font-family: inherit;'
      + '  resize: none; outline: none; min-height: 20px; max-height: 100px;'
      + '  line-height: 1.4; background: ' + CONFIG.bgLight + ';'
      + '  transition: border-color 0.15s;'
      + '}'
      + '.nova-input:focus { border-color: ' + CONFIG.primaryColor + '; background: #fff; }'
      + '.nova-input::placeholder { color: ' + CONFIG.textLight + '; }'
      + '.nova-send-btn {'
      + '  width: 38px; height: 38px; border-radius: 10px;'
      + '  background: linear-gradient(135deg, ' + CONFIG.primaryDark + ', ' + CONFIG.primaryColor + ');'
      + '  color: #fff;'
      + '  border: none; cursor: pointer;'
      + '  display: flex; align-items: center; justify-content: center;'
      + '  flex-shrink: 0;'
      + '  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);'
      + '  box-shadow: 0 2px 6px rgba(25,25,25,0.2);'
      + '}'
      + '.nova-send-btn:hover {'
      + '  transform: scale(1.05);'
      + '  box-shadow: 0 3px 10px rgba(25,25,25,0.3);'
      + '}'
      + '.nova-send-btn:active { transform: scale(0.95); }'
      + '.nova-send-btn:disabled { opacity: 0.4; cursor: not-allowed; transform: none; box-shadow: none; }'
      + '.nova-send-btn svg { width: 18px; height: 18px; }'

      // Powered by
      + '.nova-footer {'
      + '  text-align: center; padding: 6px 12px; font-size: 10px;'
      + '  color: ' + CONFIG.textLight + '; background: ' + CONFIG.bgColor + ';'
      + '  flex-shrink: 0;'
      + '  border-top: 1px solid rgba(0,0,0,0.04);'
      + '  letter-spacing: 0.3px;'
      + '}'

      // Mobile responsive
      + '@media (max-width: ' + CONFIG.mobileBreakpoint + 'px) {'
      + '  #nova-panel {'
      + '    width: calc(100vw - 16px); height: calc(100vh - 80px);'
      + '    max-height: none; bottom: 72px; right: 8px;'
      + '    border-radius: 12px;'
      + '  }'
      + '  #nova-float-btn { bottom: 12px; right: 12px; }'
      + '}';

    var styleEl = document.createElement('style');
    styleEl.id = 'nova-styles';
    styleEl.textContent = css;
    document.head.appendChild(styleEl);
  }

  // ---------------------------------------------------------------------------
  // SVG Icons
  // ---------------------------------------------------------------------------
  var ICONS = {
    chat: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
      + '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>'
      + '</svg>',
    close: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
      + '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>'
      + '</svg>',
    send: '<svg viewBox="0 0 24 24" fill="currentColor">'
      + '<path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>'
      + '</svg>',
    iq: '<svg viewBox="0 0 24 24" fill="currentColor">'
      + '<path d="M12 2L14.5 8.5L21 11L14.5 13.5L12 20L9.5 13.5L3 11L9.5 8.5L12 2Z" opacity="0.9"/>'
      + '<path d="M19 2L20 5L23 6L20 7L19 10L18 7L15 6L18 5L19 2Z" opacity="0.6"/>'
      + '</svg>',
  };

  // ---------------------------------------------------------------------------
  // Markdown renderer (lightweight, no dependencies)
  // ---------------------------------------------------------------------------
  function renderMarkdown(text) {
    if (!text) return '';
    var html = escapeHtml(text);

    // Tables (process before other inline formatting)
    html = html.replace(/^(\|.+\|)\n(\|[-:\| ]+\|)\n((?:\|.+\|\n?)*)/gm, function (match, header, sep, body) {
      var headerCells = header.split('|').filter(function (c) { return c.trim() !== ''; });
      var rows = body.trim().split('\n');
      var table = '<table><thead><tr>';
      headerCells.forEach(function (c) { table += '<th>' + c.trim() + '</th>'; });
      table += '</tr></thead><tbody>';
      rows.forEach(function (row) {
        var cells = row.split('|').filter(function (c) { return c.trim() !== ''; });
        table += '<tr>';
        cells.forEach(function (c) { table += '<td>' + c.trim() + '</td>'; });
        table += '</tr>';
      });
      table += '</tbody></table>';
      return table;
    });

    // Headers
    html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^## (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^# (.+)$/gm, '<h3>$1</h3>');

    // Bold and italic
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // Inline code
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Unordered lists
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
    html = html.replace(/((?:<li>.+<\/li>\n?)+)/g, '<ul>$1</ul>');

    // Line breaks (double newline = paragraph, single = br)
    html = html.replace(/\n\n/g, '</p><p>');
    html = html.replace(/\n/g, '<br/>');

    // Wrap in paragraph if not already wrapped
    if (html.indexOf('<h3>') !== 0 && html.indexOf('<ul>') !== 0 && html.indexOf('<table>') !== 0) {
      html = '<p>' + html + '</p>';
    }

    // Clean up empty paragraphs
    html = html.replace(/<p>\s*<\/p>/g, '');
    html = html.replace(/<p>\s*<br\/>\s*<\/p>/g, '');

    return html;
  }

  function escapeHtml(str) {
    if (str == null) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
  }

  // ---------------------------------------------------------------------------
  // Session / History persistence
  // ---------------------------------------------------------------------------
  function loadHistory() {
    try {
      var stored = sessionStorage.getItem(CONFIG.storageKey);
      return stored ? JSON.parse(stored) : [];
    } catch (e) {
      return [];
    }
  }

  function saveHistory(messages) {
    try {
      var trimmed = messages.slice(-CONFIG.maxHistoryStorage);
      sessionStorage.setItem(CONFIG.storageKey, JSON.stringify(trimmed));
    } catch (e) { /* ignore storage errors */ }
  }

  function getSessionId() {
    try {
      var sid = sessionStorage.getItem(CONFIG.sessionKey);
      if (!sid) {
        sid = 'nova-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6);
        sessionStorage.setItem(CONFIG.sessionKey, sid);
      }
      return sid;
    } catch (e) {
      return 'nova-' + Date.now();
    }
  }

  // ---------------------------------------------------------------------------
  // DOM construction
  // ---------------------------------------------------------------------------
  function buildWidget(containerId) {
    injectStyles();

    // Floating button
    var btn = document.createElement('button');
    btn.id = 'nova-float-btn';
    btn.innerHTML = ICONS.chat;
    btn.title = 'Open Nova Chat';
    btn.setAttribute('aria-label', 'Open Nova Chat');
    btn.addEventListener('click', togglePanel);
    document.body.appendChild(btn);
    state.floatingBtn = btn;

    // Chat panel
    var panel = document.createElement('div');
    panel.id = 'nova-panel';
    panel.className = 'nova-hidden';
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-label', 'Nova Chat');

    // Header
    var header = document.createElement('div');
    header.className = 'nova-header';
    header.innerHTML = ''
      + '<div class="nova-header-left">'
      + '  <div class="nova-header-icon">' + ICONS.iq + '</div>'
      + '  <div>'
      + '    <div class="nova-header-title">Nova</div>'
      + '    <div class="nova-header-subtitle">Your Recruitment Intelligence, Illuminated</div>'
      + '  </div>'
      + '</div>'
      + '<button class="nova-close-btn" aria-label="Close chat">' + ICONS.close + '</button>';
    header.querySelector('.nova-close-btn').addEventListener('click', togglePanel);
    panel.appendChild(header);

    // Messages container
    var messagesDiv = document.createElement('div');
    messagesDiv.className = 'nova-messages';
    messagesDiv.id = 'nova-messages';
    panel.appendChild(messagesDiv);

    // Input area
    var inputArea = document.createElement('div');
    inputArea.className = 'nova-input-area';

    var textarea = document.createElement('textarea');
    textarea.className = 'nova-input';
    textarea.id = 'nova-input';
    textarea.placeholder = 'Ask about recruitment marketing...';
    textarea.rows = 1;
    textarea.setAttribute('aria-label', 'Chat message input');
    textarea.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });
    textarea.addEventListener('input', function () {
      // Auto-resize
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 100) + 'px';
    });
    inputArea.appendChild(textarea);

    var sendBtn = document.createElement('button');
    sendBtn.className = 'nova-send-btn';
    sendBtn.id = 'nova-send-btn';
    sendBtn.innerHTML = ICONS.send;
    sendBtn.title = 'Send message';
    sendBtn.setAttribute('aria-label', 'Send message');
    sendBtn.addEventListener('click', sendMessage);
    inputArea.appendChild(sendBtn);
    panel.appendChild(inputArea);

    // Footer
    var footer = document.createElement('div');
    footer.className = 'nova-footer';
    footer.textContent = 'Powered by Nova \u00b7 Joveo';
    panel.appendChild(footer);

    // Add to container or body
    if (containerId) {
      var containerEl = document.getElementById(containerId);
      if (containerEl) {
        containerEl.appendChild(panel);
        state.container = containerEl;
      } else {
        document.body.appendChild(panel);
      }
    } else {
      document.body.appendChild(panel);
    }

    state.chatPanel = panel;

    // Load history
    state.messages = loadHistory();
    state.sessionId = getSessionId();

    // Render existing messages or show welcome
    if (state.messages.length > 0) {
      renderAllMessages();
    } else {
      showWelcome();
    }
  }

  // ---------------------------------------------------------------------------
  // Panel toggle
  // ---------------------------------------------------------------------------
  function togglePanel() {
    state.isOpen = !state.isOpen;
    var panel = state.chatPanel;
    if (state.isOpen) {
      panel.classList.remove('nova-hidden');
      panel.classList.add('nova-visible');
      state.floatingBtn.innerHTML = ICONS.close;
      state.floatingBtn.title = 'Close Nova Chat';
      state.floatingBtn.setAttribute('aria-label', 'Close Nova Chat');
      // Focus input
      setTimeout(function () {
        var input = document.getElementById('nova-input');
        if (input) input.focus();
      }, 300);
    } else {
      panel.classList.remove('nova-visible');
      panel.classList.add('nova-hidden');
      state.floatingBtn.innerHTML = ICONS.chat;
      state.floatingBtn.title = 'Open Nova Chat';
      state.floatingBtn.setAttribute('aria-label', 'Open Nova Chat');
    }
  }

  // ---------------------------------------------------------------------------
  // Welcome + suggestions
  // ---------------------------------------------------------------------------
  function showWelcome() {
    var messagesDiv = document.getElementById('nova-messages');
    if (!messagesDiv) return;

    // Welcome message
    var welcomeMsg = {
      role: 'assistant',
      content: 'Hello! I\'m **Nova**, your recruitment marketing intelligence assistant. '
        + 'I have access to data from **10,238+ Supply Partners**, job boards across **70+ countries**, '
        + 'and comprehensive industry benchmarks and salary data.\n\nHow can I help you today?',
      sources: [],
      confidence: 1.0,
    };
    appendMessage(welcomeMsg, false);

    // Suggestions
    var sugDiv = document.createElement('div');
    sugDiv.className = 'nova-suggestions';
    sugDiv.id = 'nova-suggestions';

    var title = document.createElement('div');
    title.className = 'nova-suggestions-title';
    title.textContent = 'Suggested questions';
    sugDiv.appendChild(title);

    SUGGESTED_QUESTIONS.forEach(function (q) {
      var btn = document.createElement('button');
      btn.className = 'nova-suggestion-btn';
      btn.textContent = q;
      btn.addEventListener('click', function () {
        var input = document.getElementById('nova-input');
        if (input) input.value = q;
        sendMessage();
      });
      sugDiv.appendChild(btn);
    });

    messagesDiv.appendChild(sugDiv);
  }

  // ---------------------------------------------------------------------------
  // Message rendering
  // ---------------------------------------------------------------------------
  function appendMessage(msg, persist) {
    var messagesDiv = document.getElementById('nova-messages');
    if (!messagesDiv) return;

    // Remove suggestions on first user message
    var sugEl = document.getElementById('nova-suggestions');
    if (sugEl && msg.role === 'user') {
      sugEl.remove();
    }

    var msgEl = document.createElement('div');
    msgEl.className = 'nova-msg nova-msg-' + msg.role;

    if (msg.role === 'assistant') {
      msgEl.innerHTML = renderMarkdown(msg.content);

      // Meta: sources + confidence
      var sources = msg.sources || [];
      var confidence = msg.confidence;
      if (sources.length > 0 || (typeof confidence === 'number' && confidence > 0)) {
        var metaDiv = document.createElement('div');
        metaDiv.className = 'nova-msg-meta';

        sources.forEach(function (src) {
          var badge = document.createElement('span');
          badge.className = 'nova-badge';
          badge.textContent = src;
          metaDiv.appendChild(badge);
        });

        if (typeof confidence === 'number' && confidence > 0) {
          var confBadge = document.createElement('span');
          var pct = Math.round(confidence * 100);
          var confClass = pct >= 75 ? 'high' : (pct >= 50 ? 'medium' : 'low');
          confBadge.className = 'nova-confidence nova-confidence-' + confClass;

          // Use structured breakdown if available
          var bd = msg.confidence_breakdown;
          if (bd && bd.grade) {
            var gradeText = 'Grade ' + bd.grade;
            var srcCount = bd.sources_count || 0;
            var freshness = bd.data_freshness || 'curated';
            var verif = bd.verification || 'unverified';
            var verifLabel = verif === 'verified' ? 'Verified' :
                             verif === 'issues_found' ? 'Issues flagged' : 'Unverified';
            confBadge.textContent = gradeText + ' \u2022 ' + srcCount + ' ' + freshness + ' source' + (srcCount !== 1 ? 's' : '');

            // Build tooltip
            var tooltip = document.createElement('div');
            tooltip.className = 'nova-confidence-tooltip';
            tooltip.innerHTML = '<div class="nova-tooltip-title">Confidence Breakdown</div>'
              + '<div class="nova-tooltip-row"><span>Overall Score</span><span>' + pct + '%</span></div>'
              + '<div class="nova-tooltip-row"><span>Grade</span><span>' + bd.grade + '</span></div>'
              + '<div class="nova-tooltip-row"><span>Data Sources</span><span>' + srcCount + ' (' + freshness + ')</span></div>'
              + '<div class="nova-tooltip-row"><span>Grounding</span><span>' + Math.round((bd.grounding_score || 0) * 100) + '%</span></div>'
              + '<div class="nova-tooltip-row"><span>Verification</span><span>' + verifLabel + '</span></div>'
              + '<div class="nova-tooltip-divider"></div>'
              + '<div class="nova-tooltip-note">Confidence is a quality signal, not a filter. '
              + 'Lower scores widen estimate ranges but do not suppress data.</div>';
            confBadge.appendChild(tooltip);
          } else {
            confBadge.textContent = pct + '% confidence';
          }
          metaDiv.appendChild(confBadge);
        }

        msgEl.appendChild(metaDiv);
      }
    } else {
      msgEl.textContent = msg.content;
    }

    messagesDiv.appendChild(msgEl);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;

    if (persist !== false) {
      state.messages.push(msg);
      saveHistory(state.messages);
    }
  }

  function renderAllMessages() {
    var messagesDiv = document.getElementById('nova-messages');
    if (!messagesDiv) return;
    messagesDiv.innerHTML = '';

    state.messages.forEach(function (msg) {
      appendMessage(msg, false);
    });
  }

  function showTyping() {
    var messagesDiv = document.getElementById('nova-messages');
    if (!messagesDiv) return;

    var typing = document.createElement('div');
    typing.className = 'nova-typing';
    typing.id = 'nova-typing';
    typing.innerHTML = ''
      + '<div class="nova-typing-dot"></div>'
      + '<div class="nova-typing-dot"></div>'
      + '<div class="nova-typing-dot"></div>';
    messagesDiv.appendChild(typing);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  }

  function hideTyping() {
    var el = document.getElementById('nova-typing');
    if (el) el.remove();
  }

  // ---------------------------------------------------------------------------
  // Send message
  // ---------------------------------------------------------------------------
  function sendMessage() {
    if (state.isLoading) return;

    var input = document.getElementById('nova-input');
    if (!input) return;

    var text = input.value.trim();
    if (!text) return;

    // Add user message
    appendMessage({ role: 'user', content: text });
    input.value = '';
    input.style.height = 'auto';

    // Disable send button
    var sendBtn = document.getElementById('nova-send-btn');
    if (sendBtn) sendBtn.disabled = true;
    state.isLoading = true;

    showTyping();

    // Build history for API
    var history = [];
    state.messages.forEach(function (m) {
      if (m.role === 'user' || m.role === 'assistant') {
        history.push({ role: m.role, content: m.content });
      }
    });

    // API call
    var payload = {
      message: text,
      conversation_id: state.sessionId,
      history: history.slice(-20),
    };
    // Include session context if set via setContext() public API
    if (CONFIG._sessionContext && typeof CONFIG._sessionContext === 'object') {
      payload.context = CONFIG._sessionContext;
    }

    // AbortController with 60-second timeout for chat requests
    var abortCtrl = new AbortController();
    var fetchTimeout = setTimeout(function () { abortCtrl.abort(); }, 60000);

    fetch(CONFIG.apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: abortCtrl.signal,
    })
      .then(function (res) {
        clearTimeout(fetchTimeout);
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function (data) {
        hideTyping();
        // Note: v2 orchestrator metadata (data_confidence, data_freshness,
        // sources_used) is available in the response payload but not yet
        // displayed in the UI. Future enhancement: show freshness badges
        // and per-source confidence breakdowns.
        appendMessage({
          role: 'assistant',
          content: data.response || 'No response received.',
          sources: data.sources || [],
          confidence: data.confidence || 0,
          confidence_breakdown: data.confidence_breakdown || null,
        });
      })
      .catch(function (err) {
        clearTimeout(fetchTimeout);
        hideTyping();
        var errorMsg = err.name === 'AbortError'
          ? 'The request timed out. Please try a shorter question or try again later.'
          : 'Sorry, I encountered an error connecting to the server. Please try again.';
        appendMessage({
          role: 'assistant',
          content: errorMsg,
          sources: [],
          confidence: 0,
        });
        console.error('Nova chat error:', err);
      })
      .finally(function () {
        state.isLoading = false;
        if (sendBtn) sendBtn.disabled = false;
        if (input) input.focus();
      });
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------
  window.NovaChat = {
    /**
     * Initialize the Nova chat widget.
     * @param {Object} options
     * @param {string} [options.containerId] - DOM element ID to mount the panel in
     * @param {string} [options.apiUrl] - Custom API endpoint URL
     * @param {string} [options.primaryColor] - Custom primary brand color
     */
    init: function (options) {
      if (state.initialized) return;
      options = options || {};

      if (options.apiUrl) CONFIG.apiUrl = options.apiUrl;
      if (options.primaryColor) CONFIG.primaryColor = options.primaryColor;

      // Wait for DOM ready
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () {
          buildWidget(options.containerId);
        });
      } else {
        buildWidget(options.containerId);
      }

      state.initialized = true;
    },

    /**
     * Programmatically open the chat panel.
     */
    open: function () {
      if (!state.isOpen && state.chatPanel) togglePanel();
    },

    /**
     * Programmatically close the chat panel.
     */
    close: function () {
      if (state.isOpen && state.chatPanel) togglePanel();
    },

    /**
     * Send a message programmatically.
     * @param {string} message
     */
    send: function (message) {
      if (!message) return;
      var input = document.getElementById('nova-input');
      if (input) {
        input.value = message;
        sendMessage();
      }
    },

    /**
     * Provide session context (e.g., from an active media plan).
     * @param {Object} context - { roles, locations, industry, budget, enriched, synthesized }
     */
    setContext: function (context) {
      if (context && typeof context === 'object') {
        CONFIG._sessionContext = context;
      }
    },

    /**
     * Clear conversation history.
     */
    clearHistory: function () {
      state.messages = [];
      saveHistory([]);
      var messagesDiv = document.getElementById('nova-messages');
      if (messagesDiv) {
        messagesDiv.innerHTML = '';
        showWelcome();
      }
    },
  };

})();
