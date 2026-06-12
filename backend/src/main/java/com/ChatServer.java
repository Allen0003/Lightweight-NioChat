package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class ChatServer {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private boolean isRunning = true;

    public ChatServer(int port) {
        this.port = port;
    }

    public void start() {
        try {
            // 1. 開啟 Selector
            this.selector = Selector.open();
            // 2. 開啟 ServerSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            // 3. 關鍵：設定為非阻塞模式 (Non-Blocking)
            this.serverSocketChannel.configureBlocking(false);
            // 4. 綁定監聽連接埠 (Port)
            this.serverSocketChannel.bind(new InetSocketAddress(port));
            // 5. 將 Server 通道註冊到 Selector，並告訴它我們目前只關心「新連線進來」(OP_ACCEPT)
            this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("高併發 NIO 聊天室伺服器已成功啟動，監聽 Port: " + port);

            // 6. 進入核心的 Event Loop (事件輪詢主迴圈)
            while (isRunning) {
                // 此方法會阻塞，直到作業系統通知至少有一個通道的事件就緒
                int readyChannels = selector.select();
                if (readyChannels == 0) continue;

                // 獲取所有就緒事件的 SelectionKey 集合
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    // !! 必須手動將處理過的 key 從集合中移除，否則下次輪詢會重複處理 !!
                    keyIterator.remove();
                    // 檢查這個 key 目前處於什麼就緒狀態
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        // 有新的客戶端嘗試連線
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        // 有客戶端發送資料過來，通道可讀
                        handleRead(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeServer();
        }
    }

    private void handleAccept(SelectionKey key) {
        try {
            // 1. 從 SelectionKey 中獲取原先的 ServerSocketChannel
            ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

            // 2. 接受客戶端的連線（此時建立 TCP 三向交握）
            // 因為 ServerSocketChannel 是非阻塞的，若無連線此處會回傳 null，但既然 key.isAcceptable() 成立，這裡一定有連線
            SocketChannel clientChannel = serverChannel.accept();

            // 3. 關鍵：一定要將客戶端通道也設為非阻塞
            clientChannel.configureBlocking(false);

            // 4. 將客戶端通道註冊到同一個 Selector，開始監聽「可讀（OP_READ）」事件
            // 我們可以順便附帶一個物件（Attachment）作為這個通道的識別，這裡先附帶客戶端的遠端地址
            clientChannel.register(this.selector, SelectionKey.OP_READ, clientChannel.getRemoteAddress());

            System.out.println("成功連線來自客戶端: " + clientChannel.getRemoteAddress());

        } catch (IOException e) {
            System.err.println("處理新連線時發生異常: " + e.getMessage());
        }
    }

    private void handleRead(SelectionKey key) {
        // 1. 從 SelectionKey 中獲取觸發事件的客戶端通道
        SocketChannel clientChannel = (SocketChannel) key.channel();
        // 2. 分配一個 1024 位元組的緩衝區
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            // 3. 從通道讀取資料到 Buffer 中
            int bytesRead = clientChannel.read(buffer);

            // 如果回傳 -1，代表客戶端主動斷開連線（完成了 TCP 四次揮手）
            if (bytesRead == -1) {
                disconnect(key, clientChannel);
                return;
            }

            // 4. 關鍵！切換 Buffer 為「讀取模式」（倒帶）
            buffer.flip();

            // 5. 將 Buffer 中的位元組轉換為字串
            String message = StandardCharsets.UTF_8.decode(buffer).toString();
            String clientAddress = key.attachment().toString(); // 拿回當初 accept 時附帶的地址

            System.out.println("[" + clientAddress + "]: " + message.trim());

            // 6. 廣播給其他所有在線的客戶端
            broadcast(message, clientChannel);

        } catch (IOException e) {
            // 發生異常（例如客戶端強行關閉），進行斷開處理
            disconnect(key, clientChannel);
        }
    }

    private void broadcast(String message, SocketChannel excludeChannel) {
        // 取得目前 Selector 監管的所有通道金鑰
        for (SelectionKey key : selector.keys()) {
            // 我們只想廣播給「客戶端通道」，必須排除 ServerSocketChannel 自己和發送者自己
            if (key.isValid() && key.channel() instanceof SocketChannel) {
                SocketChannel targetChannel = (SocketChannel) key.channel();
                if (targetChannel != excludeChannel) {
                    try {
                        // 將字串包裝成 ByteBuffer
                        ByteBuffer writeBuffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
                        // 直接寫入通道發送
                        targetChannel.write(writeBuffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void disconnect(SelectionKey key, SocketChannel clientChannel) {
        try {
            System.out.println("客戶端已中斷連線: " + clientChannel.getRemoteAddress());
            // 取消這個 key 在 Selector 中的註冊
            key.cancel();
            // 關閉通道
            clientChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeServer() {
        try {
            if (serverSocketChannel != null) serverSocketChannel.close();
            if (selector != null) selector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 啟動在 8080 埠
        new ChatServer(8080).start();
    }
}