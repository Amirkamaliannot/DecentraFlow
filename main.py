from p2p_node import P2PNode
import time

# مثال استفاده
if __name__ == "__main__":
    # شروع Node
    node = P2PNode()
    node.start()
    
    # مثال: تقسیم و ذخیره یک متن
    sample_text = """
        Lorem ipsum was conceived as filler text, form for for for atted in a certain way to enable the presentation of graphic elements in documents, without the need for fo
    """
    
    # print("\n📝 شروع پردازش...")
    # chunk_hashes = node.split_and_store(sample_text, chunk_size=50)
    
    # # اجرای MapReduce برای شمارش کلمات
    # result = node.map_reduce(chunk_hashes, task_type='word_count')
    
    # print("\n📊 نتیجه شمارش کلمات:")
    # for word, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
    #     print(f"  {word}: {count}")
    
    # نگه داشتن Node برای تست
    input("\n⏸️  Enter for exit ...")
    node.stop()