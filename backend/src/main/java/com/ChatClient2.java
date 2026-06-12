package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Scanner;

public class ChatClient2 {
    private final String host;
    private final int port;
    private SocketChannel socketChannel;
    private Selector selector;

    public ChatClient2(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        try {
            this.selector = Selector.open();
            // 1. 連線到伺服器
            this.socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
            this.socketChannel.configureBlocking(false); // 網路端維持非阻塞
            // 註冊網路讀取事件，並掛載一個客戶端專用的 2KB 拆包暫存區
            ByteBuffer receiveBuffer = ByteBuffer.allocate(2048);
            this.socketChannel.register(selector, SelectionKey.OP_READ, receiveBuffer);

            System.out.println("成功連接高併發 NIO 聊天室！");

            // 2. 開啟分支執行緒：專門負責非同步「接收」伺服器的廣播（網路端）
            startNetworkListener();

            // 3. 主執行緒：專門負責「發送」（控制台輸入端）
            // 這裡直接使用 Scanner 阻塞等待鍵盤，架構極其清晰
            Scanner scanner = new Scanner(System.in);
            System.out.println("請輸入訊息並按下 Enter 發送：");

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.trim().isEmpty()) continue;

                // 依照通訊協定發送字串
                sendProtocolMessage(line);
            }
        } catch (IOException e) {
            System.err.println("連線失敗: " + e.getMessage());
        }
    }

    // 核心發送邏輯：打包成 [4位元組長度] + [本文]
    private void sendProtocolMessage(String message) throws IOException {
        byte[] bodyBytes = message.getBytes(StandardCharsets.UTF_8);
        int totalLength = bodyBytes.length;

        // 分配一個剛好容納標頭與本文的 Buffer
        ByteBuffer writeBuffer = ByteBuffer.allocate(4 + totalLength);
        writeBuffer.putInt(totalLength); // 寫入 4 位元組長度
        writeBuffer.put(bodyBytes);      // 寫入本文
        writeBuffer.flip();              // 切換為讀取模式

        // 確保完全寫入網路通道
        while (writeBuffer.hasRemaining()) {
            socketChannel.write(writeBuffer);
        }
    }

    // 分支執行緒：專門處理網路事件輪詢
    private void startNetworkListener() {
        Thread networkThread = new Thread(() -> {
            try {
                while (true) {
                    int readyChannels = selector.select();
                    if (readyChannels == 0) continue;

                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();

                        if (key.isReadable()) {
                            handleServerMessage(key);
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("網路監聽執行緒異常關閉。");
            }
        });
        networkThread.setDaemon(true); // 設為守護執行緒
        networkThread.start();
    }

    // 核心接收邏輯：解碼來自伺服器的 [4位元組長度] + [本文]（解決粘包/半包）
    private void handleServerMessage(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                System.out.println("\n伺服器已關閉，連線中斷。");
                System.exit(0);
            }

            // 滾動式拆包
            while (true) {
                buffer.flip();

                if (buffer.remaining() < 4) {
                    buffer.compact();
                    break;
                }

                buffer.mark();
                int messageLength = buffer.getInt();

                if (buffer.remaining() < messageLength) {
                    buffer.reset();
                    buffer.compact();
                    break;
                }

                byte[] bodyBytes = new byte[messageLength];
                buffer.get(bodyBytes);

                String msg = new String(bodyBytes, StandardCharsets.UTF_8);
                // 直接將接收到的廣播印在控制台上
                System.out.println("\n[廣播]: " + msg);
                System.out.print("請輸入訊息并按下 Enter 發送：");

                buffer.compact();
            }

        } catch (IOException e) {
            System.out.println("\n與伺服器斷開連線。");
            System.exit(0);
        }
    }

    public void testHalfPacket() {
        try {
            this.socketChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 8080));
            this.socketChannel.configureBlocking(false);
            System.out.println("成功連線，開始製造【半包】惡意測試...");

            String body = "Hello World";
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

            // 總共需要發送 4 + 11 = 15 個位元組
            ByteBuffer buffer = ByteBuffer.allocate(4 + bodyBytes.length);
            buffer.putInt(bodyBytes.length); // 4位元組長度
            buffer.put(bodyBytes);           // 11位元組本文
            buffer.flip();

            // --- 惡意切片開始 ---
            // 我們先強行只發送前 7 個位元組（4位元組長度標頭 + "Hel" 3個位元組）
            byte[] firstPart = new byte[7];
            buffer.get(firstPart);

            System.out.println("--> 發送前半段 (4字節長度 + 'Hel')...");
            socketChannel.write(ByteBuffer.wrap(firstPart));

            // 故意讓執行緒睡 3 秒鐘，這期間 Server 的 handleRead 必然會被觸發一次！
            System.out.println("--> 故意凍結 3 秒，模擬網路延遲（半包狀態）...");
            Thread.sleep(3000);

            // 3秒後，發送剩下的 8 個位元組 ("lo World")
            byte[] secondPart = new byte[buffer.remaining()];
            buffer.get(secondPart);

            System.out.println("--> 發送後半段 ('lo World')...");
            socketChannel.write(ByteBuffer.wrap(secondPart));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void testStickyPacket() {
        try {
            this.socketChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 8080));
            this.socketChannel.configureBlocking(false);
            System.out.println("成功連線，開始製造【粘包】惡意測試...");

            byte[] body1 = "MessageAAA".getBytes(StandardCharsets.UTF_8);
            byte[] body2 = "MessageBBB".getBytes(StandardCharsets.UTF_8);

            // 準備一個大 Buffer，把兩條訊息「物理黏死」在一起
            // 總長度 = (4 + 10) + (4 + 10) = 28 位元組
            ByteBuffer stickyBuffer = ByteBuffer.allocate((4 + body1.length) + (4 + body2.length));

            // 塞入第一條
            stickyBuffer.putInt(body1.length);
            stickyBuffer.put(body1);

            // 緊接著塞入第二條，完全沒有間隔
            stickyBuffer.putInt(body2.length);
            stickyBuffer.put(body2);

            stickyBuffer.flip();

            System.out.println("--> 用單次 write() 同時發送黏在一起的兩條訊息...");
            socketChannel.write(stickyBuffer);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 執行半包測試
//        new ChatClient2("127.0.0.1", 8080).testHalfPacket();
        new ChatClient2("127.0.0.1", 8080).testStickyPacket();
    }
}