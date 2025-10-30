from p2p_node import P2PNode
from DFlow import DFlow ,DFlowManager, add_file_as_dflow
from time import sleep


chunk_size = 512

def initiate_dflow(path:str, Manager:DFlowManager):
    dflow = add_file_as_dflow(Manager, path, items_per_chunk=chunk_size, mode='line')
    return dflow


if __name__ == "__main__":

    node = P2PNode()
    manager = DFlowManager('dflows_real.json')
    node.start()

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
                initiate_dflow(path, manager)
            else:
                print("<file> parameter required")            
                
        if(instruction[0] == "/attach-Dflow"):
            if len(instruction) > 1 and instruction[1]!="" :
                hash = instruction[1]
                
            else:
                print("<file> parameter required")  
        
        if(instruction[0] == "/list-Dflow"):
            for i in manager.list_all():
                print(i , ":", i.file_hash)        
                
        if(instruction[0] == "/exit"):
            break

        sleep(0.1)
    
    input("\n⏸️  Enter for exit ...")
    node.stop()