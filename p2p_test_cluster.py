import time
import threading
from collections import defaultdict

# کپی کردن کلاس P2PNode از فایل قبلی
# (در پروژه واقعی، این را از ماژول جداگانه import کنید)

class SimpleP2PCluster:
    """مدیریت کلاستر از چند Node"""
    
    def __init__(self, num_nodes=3, base_port=5000):
        self.nodes = []
        self.base_port = base_port
        
        # ایجاد Nodeها
        for i in range(num_nodes):
            from p2p_hadoop_node import P2PNode  # فرض: فایل قبلی ذخیره شده
            node = P2PNode(port=base_port + i)
            self.nodes.append(node)
    
    def start_all(self):
        """شروع تمام Nodeها"""
        print("🚀 شروع کلاستر...")
        for i, node in enumerate(self.nodes):
            node.start()
            print(f"  ✓ Node {i+1} آماده شد")
            time.sleep(0.5)
        
        # زمان برای کشف همتایان
        print("\n🔍 کشف همتایان...")
        time.sleep(3)
        
        for i, node in enumerate(self.nodes):
            print(f"  Node {i+1}: {len(node.peers)} همتا پیدا شد")
    
    def distribute_file(self, filepath):
        """توزیع فایل در کلاستر"""
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # استفاده از اولین Node برای تقسیم
        print(f"\n📤 توزیع فایل: {filepath}")
        chunk_hashes = self.nodes[0].split_and_store(content, chunk_size=200)
        
        return chunk_hashes
    
    def run_distributed_wordcount(self, chunk_hashes):
        """اجرای word count به صورت توزیع شده"""
        print("\n⚙️  اجرای پردازش توزیع‌شده...")
        
        # هر Node قسمتی از chunkها را پردازش می‌کند
        results = []
        chunks_per_node = len(chunk_hashes) // len(self.nodes) + 1
        
        threads = []
        for i, node in enumerate(self.nodes):
            start_idx = i * chunks_per_node
            end_idx = min((i + 1) * chunks_per_node, len(chunk_hashes))
            node_chunks = chunk_hashes[start_idx:end_idx]
            
            if node_chunks:
                thread = threading.Thread(
                    target=lambda n, c, r: r.append(n.map_reduce(c, 'word_count')),
                    args=(node, node_chunks, results)
                )
                threads.append(thread)
                thread.start()
        
        # منتظر اتمام تمام threadها
        for thread in threads:
            thread.join()
        
        # Reduce نهایی - ترکیب نتایج همه Nodeها
        print("\n🔗 ترکیب نتایج...")
        final_result = defaultdict(int)
        for result in results:
            for word, count in result.items():
                final_result[word] += count
        
        return dict(final_result)
    
    def show_cluster_stats(self):
        """نمایش آمار کلاستر"""
        print("\n📊 آمار کلاستر:")
        total_chunks = 0
        for i, node in enumerate(self.nodes):
            num_chunks = len(node.chunks)
            total_chunks += num_chunks
            print(f"  Node {i+1} (:{node.port}): {num_chunks} chunk، {len(node.peers)} peer")
        print(f"  جمع کل: {total_chunks} chunk در کلاستر")
    
    def stop_all(self):
        """توقف تمام Nodeها"""
        print("\n🛑 توقف کلاستر...")
        for node in self.nodes:
            node.stop()


def demo_wordcount():
    """نمایش عملکرد با یک مثال word count"""
    
    # ایجاد یک فایل تست
    test_content = """
    داستان پردازش توزیع شده
    
    روزی روزگاری سیستم‌های متمرکز حاکم بودند
    اما با رشد داده‌ها نیاز به سیستم‌های توزیع شده پیدا شد
    
    در سیستم‌های توزیع شده هر گره می‌تواند پردازش انجام دهد
    این باعث افزایش سرعت و مقیاس‌پذیری می‌شود
    
    Hadoop و Spark از معروف‌ترین سیستم‌های پردازش توزیع شده هستند
    اما در این پروژه ما یک نسخه ساده peer-to-peer ساختیم
    
    در معماری peer-to-peer هیچ نقطه مرکزی وجود ندارد
    همه گره‌ها با هم برابر هستند و می‌توانند با یکدیگر ارتباط برقرار کنند
    """
    
    with open('test_file.txt', 'w', encoding='utf-8') as f:
        f.write(test_content)
    
    # شروع کلاستر
    cluster = SimpleP2PCluster(num_nodes=3, base_port=5000)
    cluster.start_all()
    
    # توزیع فایل
    chunk_hashes = cluster.distribute_file('test_file.txt')
    
    # نمایش آمار
    cluster.show_cluster_stats()
    
    # اجرای word count
    result = cluster.run_distributed_wordcount(chunk_hashes)
    
    # نمایش نتایج
    print("\n📈 نتایج نهایی - بیشترین کلمات تکرار شده:")
    sorted_words = sorted(result.items(), key=lambda x: x[1], reverse=True)
    for word, count in sorted_words[:15]:
        if len(word) > 2:  # فقط کلمات معنی‌دار
            print(f"  {word}: {count} بار")
    
    # توقف
    input("\n⏸️  Enter بزنید برای خروج...")
    cluster.stop_all()


def demo_comparison():
    """مقایسه سرعت با/بدون توزیع"""
    import random
    import string
    
    # تولید یک متن بزرگ
    def generate_text(size_kb):
        words = ['داده', 'پردازش', 'توزیع', 'شده', 'سیستم', 'کلاستر', 
                 'محاسبات', 'موازی', 'گره', 'شبکه']
        text = ' '.join(random.choice(words) for _ in range(size_kb * 50))
        return text
    
    text = generate_text(10)  # 10KB متن
    
    with open('large_test.txt', 'w', encoding='utf-8') as f:
        f.write(text)
    
    print("🏁 تست مقایسه‌ای سرعت")
    print("="*50)
    
    # تست با 1 Node
    print("\n1️⃣  پردازش با 1 Node...")
    cluster1 = SimpleP2PCluster(num_nodes=1, base_port=5010)
    cluster1.start_all()
    
    start_time = time.time()
    chunks1 = cluster1.distribute_file('large_test.txt')
    result1 = cluster1.run_distributed_wordcount(chunks1)
    time1 = time.time() - start_time
    
    cluster1.stop_all()
    print(f"  ⏱️  زمان: {time1:.2f} ثانیه")
    
    time.sleep(2)
    
    # تست با 3 Node
    print("\n3️⃣  پردازش با 3 Node...")
    cluster3 = SimpleP2PCluster(num_nodes=3, base_port=5020)
    cluster3.start_all()
    
    start_time = time.time()
    chunks3 = cluster3.distribute_file('large_test.txt')
    result3 = cluster3.run_distributed_wordcount(chunks3)
    time3 = time.time() - start_time
    
    cluster3.stop_all()
    print(f"  ⏱️  زمان: {time3:.2f} ثانیه")
    
    # نمایش نتیجه
    speedup = time1 / time3 if time3 > 0 else 0
    print(f"\n🚀 افزایش سرعت: {speedup:.2f}x")


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════╗
    ║  سیستم پردازش توزیع شده P2P        ║
    ║  Decentralized Hadoop-like System   ║
    ╚══════════════════════════════════════╝
    """)
    
    print("انتخاب کنید:")
    print("1. نمایش Word Count توزیع شده")
    print("2. تست مقایسه سرعت")
    print("3. هر دو")
    
    choice = input("\nانتخاب (1/2/3): ").strip()
    
    if choice == '1':
        demo_wordcount()
    elif choice == '2':
        demo_comparison()
    elif choice == '3':
        demo_wordcount()
        print("\n" + "="*50 + "\n")
        demo_comparison()
    else:
        print("انتخاب نامعتبر!")