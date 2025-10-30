from p2p_node import P2PNode
from DFlow import DFlow ,DFlowManager, add_file_as_dflow
from time import sleep
from setting import chunk_size



if __name__ == "__main__":

    node = P2PNode()
    manager = DFlowManager('dflows_real.json')
    node.start()

    nodeID = node.nodeID

    while(node.running):

        command = input("Enter command (h for help): \n").strip()
        instruction = command.split()
        if(command in ["h", 'help']):
            print(
                "/create-Dflow <file>: creating new job" + "\n" 
                "/attach-Dflow <Dflow-hash>: attching to a job"+ "\n"
                "/list-Dflow "+ "\n"
        )
            
        if(instruction[0] == "/create-Dflow"):

            if len(instruction) > 1 and instruction[1]!="" :
                path = instruction[1]
                dflow = add_file_as_dflow(manager, path, items_per_chunk=chunk_size, mode='line')
            else:
                print("<file> parameter required")            
                
        if(instruction[0] == "/attach-Dflow"):
            if len(instruction) > 1 and instruction[1]!="" :
                hash = instruction[1]
                dflow = manager.get_by_hash(hash)
                if(not dflow): 
                    print("❌ DFlow not founded!")
                else:
                    while(True):
                        sec_comn = input (f"{dflow.file_hash}: ... (h for help)")
                        if(sec_comn in ['help', 'h']):
                            print(                
                            "/start : start flow" + "\n" 
                            "/status : get status of chunks"+ "\n"
                            "/exit : back"+ "\n"
                            )
                        elif(sec_comn in ['/start']):
                            dflow.fill_chunks_queue()
                        elif(sec_comn in ['/status']):
                            print()
                            if(dflow.fileHandle): print('Local')
                            print(dflow.chunk_size)
                            print(dflow.total_chunks)
                        elif(sec_comn in ['/exit']):
                            break
                        

            else:
                print("<file> parameter required")  
        
        if(instruction[0] == "/list-Dflow"):
            print(1)
            for i in manager.list_all():
                print(i , ":", i.file_hash, i.fileHandle)       
                
        if(instruction[0] == "/exit"):
            break

        sleep(0.1)
    
    input("\n⏸️  Enter for exit ...")
    node.stop()