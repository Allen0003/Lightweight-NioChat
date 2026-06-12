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
            ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
            SocketChannel clientChannel = serverChannel.accept();
            clientChannel.configureBlocking(false);

            // 核心改動：為這個客戶端配置一個 2KB 的專屬暫存區，並作為 Attachment 掛載
            // 預設為「寫入模式」
            ByteBuffer clientBuffer = ByteBuffer.allocate(2048);
            clientChannel.register(this.selector, SelectionKey.OP_READ, clientBuffer);
            System.out.println("成功連線來自客戶端: " + clientChannel.getRemoteAddress());
        } catch (IOException e) {
            System.err.println("處理新連線時發生異常: " + e.getMessage());
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        // 1. 拿回這個連線專屬的持久型暫存區 (此時 buffer 處於寫入模式)
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        try {
            // 2. 將網路線上的新資料追加 (Append) 到暫存區中
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead == -1) {
                disconnect(key, clientChannel);
                return;
            }

            // 3. 進入滾動式拆包迴圈。因為可能一次黏了很多條訊息，我們必須用 while 榨乾它
            while (true) {
                // 切換成「讀取模式」來檢查裡面的資料
                buffer.flip();

                // 狀況 A：如果連 4 位元組的長度標頭都不夠，代表資料還太少
                if (buffer.remaining() < 4) {
                    // 還原成「寫入模式」，保留現有資料，等下一次 OP_READ 觸發
                    buffer.compact();
                    break;
                }
                // 標記目前位置，如果等一下發現內容不夠（半包），可以回滾
                buffer.mark();

                // 讀取前 4 碼，得知後面本文的預期長度
                int messageLength = buffer.getInt();

                // 狀況 B：如果剩下的資料小於本文預期長度（發生半包）
                if (buffer.remaining() < messageLength) {
                    // 回滾到 mark 的位置（把剛剛 readInt 消耗掉的 4 位元組吐回去）
                    buffer.reset();
                    // 還原成「寫入模式」，保留現有資料，等下一次網路資料進來
                    buffer.compact();
                    break;
                }

                // 狀況 C：資料夠了！精準截取指定長度的位元組
                byte[] bodyBytes = new byte[messageLength];
                buffer.get(bodyBytes); // 從 buffer 讀出本文

                String message = new String(bodyBytes, StandardCharsets.UTF_8);
                System.out.println("來自 [" + clientChannel.getRemoteAddress() + "] 的完整訊息: " + message.trim());

                // 廣播給其他人
                broadcast(message, clientChannel);
                // 核心關鍵：將已經處理完的資料剔除，未處理的資料（粘包的下一條）移到最前面
                // compact() 會自動把 position 設在未處理資料的後面，讓 buffer 回到「寫入模式」
                buffer.compact();
                // 繼續 while 迴圈，檢查緊跟在後面的下一條訊息是不是也是完整的
            }
        } catch (IOException e) {
            disconnect(key, clientChannel);
        }
    }

    private void broadcast(String message, SocketChannel excludeChannel) {
        byte[] bodyBytes = message.getBytes(StandardCharsets.UTF_8);
        int totalLength = bodyBytes.length;

        // 分配一個剛好容納 [4位元組長度標頭 + 本文] 的 Buffer
        ByteBuffer writeBuffer = ByteBuffer.allocate(4 + totalLength);
        writeBuffer.putInt(totalLength); // 先寫入 4 碼長度
        writeBuffer.put(bodyBytes);      // 再寫入本文
        writeBuffer.flip();              // 切換為讀取模式以供寫出

        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.channel() instanceof SocketChannel) {
                SocketChannel targetChannel = (SocketChannel) key.channel();
                if (targetChannel != excludeChannel) {
                    try {
                        // 為了確保完整寫出，重置 writeBuffer 的 position 到開頭
                        writeBuffer.rewind();
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