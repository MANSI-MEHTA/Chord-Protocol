import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

public class BootstrapServer extends Thread {

	//Map to store name server id and corresponding name server
	static SortedMap<Integer, NS> cHashSystem = new TreeMap<>();
	
	//Map to store key-value pairs
	static SortedMap<Integer, String> keyValMap = new TreeMap<>();
	
	public static int id;
	public static int bsport;

	public BootstrapServer(int id, int port) {
		this.id = id;
		this.bsport = port;
	}

	public static void addNameServer(int id, int port) {
		BootstrapServer.NS ns = new BootstrapServer.NS(port);
		cHashSystem.put(id, ns);
	}

//	public static void insert(int key, String val) {
//        if (keyValMap.containsKey(key)) {
//            System.out.println("Key already exists!");
//        } else {
//        	keyValMap.put(key, val);
//            if (!BootstrapServer.cHashSystem.isEmpty()){
//                for (int k : BootstrapServer.cHashSystem.keySet()) {
//                    System.out.println("Checking server:"+k);
//                    BootstrapServer.NS ns_check=cHashSystem.get(k);
//                    int firstKey=ns_check.firstKey;
//                    int lastkey=ns_check.lastKey;
//                    if(key>=firstKey && key<=lastkey){
//                        System.out.println("Insertion of key at server:"+k);
//                        ns_check.getPairs().put(key,val);
//                        System.out.println("Key-value pair inserted successfully!");
//                        break;
//                    }
//                }
//            }
//        }
//    }

	
	public static void insert(int key, String val) {
		if (!BootstrapServer.cHashSystem.isEmpty()) {
			for (int k : BootstrapServer.cHashSystem.keySet()) {
				System.out.println("Checking server:" + k);
				BootstrapServer.NS ns_check = cHashSystem.get(k);
				int firstKey = ns_check.firstKey;
				int lastkey = ns_check.lastKey;
				if (key >= firstKey && key <= lastkey) {
					if (ns_check.getPairs().containsKey(key)) {
						System.out.println("Key already exists!");
						break;
					}
					System.out.println("Insertion of key at server:" + k);
					ns_check.getPairs().put(key, val);
					System.out.println("Key-value pair inserted successfully!");
					break;
				}
			}
		}
	}

	public static void delete(int key) {
		boolean deleted = false;
		if (keyValMap.isEmpty()) {
			System.out.println("Key not found or does not exist!");
		}
		int id = 0;
		if (!BootstrapServer.cHashSystem.isEmpty()) {
			for (int k : BootstrapServer.cHashSystem.keySet()) {
				id = k;
				System.out.println("Checking server:" + id);
				BootstrapServer.NS ns_check = cHashSystem.get(k);
				int firstKey = ns_check.firstKey;
				int lastkey = ns_check.lastKey;
				if (key >= firstKey && key <= lastkey) {
					if(ns_check.getPairs().keySet().contains(key)) {
						ns_check.getPairs().remove(key);
						System.out.println("Found at server:" + id);
						System.out.println("Successful deletion!");
						deleted = true;
						break;
					}else {
						System.out.println("Key not found");
						return;
					}
				}
			}
		}

		if (!deleted) {
			System.out.println("Key not found");
		}
	}
	
	public static void lookup(int key) {
		boolean exists=false;
		if (keyValMap.isEmpty()) {
			System.out.println("Key not found or does not exist!");
		}
	
        if (!BootstrapServer.cHashSystem.isEmpty()){
            for (int k : BootstrapServer.cHashSystem.keySet()) {
                System.out.println("Checking server:"+k);
                BootstrapServer.NS ns_check=cHashSystem.get(k);
                int firstKey=ns_check.firstKey;
                int lastkey=ns_check.lastKey;
                if(key>=firstKey && key<=lastkey){
                	for (int x : ns_check.getPairs().keySet()) {
                		if (x == key) {
    						System.out.println("Found at server:" + k);
    						System.out.println("value of the key:" + ns_check.getPairs().get(x));
    						exists = true;
    						break;
    					}
                	}
                	if(exists)
                    	break;
                    else {
                    	System.out.println("Key not found");
                    	return;
                    }
                }  
            }//for
        }//if
    }
	
	public void run() {
		while (true) {
			Scanner sc = new Scanner(System.in);
			System.out.println("Enter command:");
			String input = sc.nextLine();
			String[] commandStr = input.split(" ");
			int lookupkey;
			String ins_val;

			switch (commandStr[0]) { 
			case "lookup":
				lookupkey = Integer.parseInt(commandStr[1]);
				lookup(lookupkey);
				break;
			case "insert":
				lookupkey = Integer.parseInt(commandStr[1]);
				ins_val = commandStr[2];
				insert(lookupkey, ins_val);
				break;
			case "delete":
				lookupkey = Integer.parseInt(commandStr[1]);
				delete(lookupkey);
				break;
			default:
				System.out.println("Invalid input command!");
				break;
			}// switch
		}
	}// run

	public static void main(String[] args) {

		String configFileName = args[0];
		File configFile = new File(configFileName);
		
		//Create key and value arrays of size 1024
		int[] keys = new int[1024];
		String[] vals = new String[1024];
		int i = 0;
		String line;
		
		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
			int bsid = Integer.parseInt(br.readLine());
			int bsport = Integer.parseInt(br.readLine());
			line = br.readLine();
			
			//Populate the key and value arrays
			while (line != null) {
				String[] pair = line.split(" ");
				keys[i] = Integer.parseInt(pair[0]);
				vals[i] = pair[1];
				i++;
				line = br.readLine();
			} 

			//Copy the values into a sorted map
			for (int k = 0; k < i; k++) {
				keyValMap.put(keys[k], vals[k]);
			}
			
			BootstrapServer.addNameServer(bsid, bsport);
			BootstrapServer.NS bns = BootstrapServer.cHashSystem.get(bsid);
			bns.setPairs(keyValMap);
			bns.setFirstKey(0);
			bns.setLastKey(1023);
			
			Thread t = new BootstrapServer(bsid, bsport);
			t.start();
			
			BootstrapServerDriver bsd = new BootstrapServerDriver(bsport);
			bsd.connectNS();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static class NS {
		int port;
		SortedMap<Integer, String> pairs = new TreeMap<>();
		int firstKey;
		int lastKey;
		int updatedSuccId;
		int updatedPrevId;
		
		NS(int port) {
			this.port = port;
		}

		public int getPort() {
			return port;
		}

		public SortedMap<Integer, String> getPairs() {
			return pairs;
		}

		public void setPairs(SortedMap<Integer, String> pairs) {
			this.pairs = pairs;
		}

		public int getFirstKey() {
			return firstKey;
		}

		public void setFirstKey(int firstKey) {
			this.firstKey = firstKey;
		}

		public int getLastKey() {
			return lastKey;
		}

		public void setLastKey(int lastKey) {
			this.lastKey = lastKey;
		}
		
		public int getUpdatedSuccId() {
			return updatedSuccId;
		}
		
		public void setUpdatedSuccId(int updatedSuccId) {
			this.updatedSuccId = updatedSuccId;
		}
		
		public int getUpdatedPrevId() {
			return updatedPrevId;
		}
		
		public void setUpdatedPrevId(int updatedPrevId) {
			this.updatedPrevId = updatedPrevId;
		}
	}
}


/*
 * This class creates a server socket connection
 */
class BootstrapServerDriver {

	int bsport;
	ServerSocket bsSocket;
	Socket clientSocket;

	BootstrapServerDriver(int bsport) {
		this.bsport = bsport;
	}

	public void connectNS() {
		try {
			bsSocket = new ServerSocket(bsport);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Waiting for new name servers to connect");
		while (true) {	
			try {
				clientSocket = bsSocket.accept();
				NSThread nst = new NSThread(clientSocket, bsSocket);
				nst.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

/*
 * This class handles the nameserver commands: enter and exit
 */
class NSThread extends Thread {

	ServerSocket bsSocket;
	Socket clientSocket;
	DataOutputStream dos;
	DataInputStream dis;
	
	NSThread(Socket clientS, ServerSocket bsSocket) {
		this.clientSocket = clientS;
		this.bsSocket = bsSocket;
	}

	public void run() {
		try {
			dis = new DataInputStream(clientSocket.getInputStream());
			dos = new DataOutputStream(clientSocket.getOutputStream());
			
			int nsid;
			while (true) {
				String input = dis.readUTF();
				String[] cmd = input.split(" ");
				switch (cmd[0]) {
				case "enter":
					nsid = Integer.parseInt(cmd[1]);
					enter(nsid);
					break;
				case "exit":
					nsid = Integer.parseInt(cmd[1]);
					exit(cmd);
					break;
				default:
					System.out.println("Invalid input");
				}
				if (cmd[0].equals("exit")) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void enter(int nsid) {
		StringBuilder servers = new StringBuilder();
		try {
			int nsport = clientSocket.getPort();
			String nsip = clientSocket.getInetAddress().toString();
			int previd = 0, succid = 0, prevport = BootstrapServer.bsport, succport = BootstrapServer.bsport;
			if (!BootstrapServer.cHashSystem.isEmpty()) {
				for (int key : BootstrapServer.cHashSystem.keySet()) {
					if (nsid > key) {
						previd = key;
						BootstrapServer.NS ns = BootstrapServer.cHashSystem.get(key);
						prevport = ns.getPort();
						servers.append(key + " ");
						continue;
					} else {
						succid = key;
						BootstrapServer.NS ns = BootstrapServer.cHashSystem.get(key);
						succport = ns.getPort();
						break;
					}
				}
			}
			dos.writeUTF(String.valueOf(previd));
			dos.writeUTF(String.valueOf(prevport));
			dos.writeUTF(String.valueOf(succid));
			dos.writeUTF(String.valueOf(succport));

			SortedMap<Integer, String> sub1 = new TreeMap<>();
			SortedMap<Integer, String> sub2 = new TreeMap<>();
			for (int key : BootstrapServer.keyValMap.keySet()) {
				if (key > previd && key <= nsid) {
					sub1.put(key, BootstrapServer.keyValMap.get(key));
				}
				if (key > nsid && key <= succid) {
					sub2.put(key, BootstrapServer.keyValMap.get(key));
				}
			}

			if (sub2.isEmpty()) {
				for (int key : BootstrapServer.keyValMap.keySet()) {
					if (key > nsid && nsid > succid) {
						sub2.put(key, BootstrapServer.keyValMap.get(key));
					}
				}
			}

			BootstrapServer.addNameServer(nsid, nsport);

			BootstrapServer.NS ns1 = BootstrapServer.cHashSystem.get(nsid);
			ns1.setPairs(sub1);
			int firstKey=previd+1<sub1.firstKey()?previd+1: sub1.firstKey();
			int lastKey=nsid>sub1.lastKey()?nsid: sub1.lastKey();	
			ns1.setFirstKey(firstKey);
			ns1.setLastKey(lastKey);
			
			System.out.println("Current key :"+ns1.firstKey+" "+ns1.getLastKey());

			BootstrapServer.NS ns = BootstrapServer.cHashSystem.get(succid);
			ns.setPairs(sub2);
			ns.setFirstKey(nsid+1<sub2.firstKey()? nsid+1:sub2.firstKey());
			ns.setLastKey(succid>sub2.lastKey()? succid:sub2.lastKey());
			ns.setUpdatedPrevId(nsid);
			System.out.println("Succ key :"+ns.firstKey+" "+ns.getLastKey());
			
			//Update successor of prev node as current node
			BootstrapServer.NS nsPrev= BootstrapServer.cHashSystem.get(previd);
			nsPrev.setUpdatedSuccId(nsid);
			
			ns1.setUpdatedPrevId(previd);
			ns1.setUpdatedSuccId(succid);
			
			System.out.println("Current node :"+nsid+" "+ns1.getUpdatedPrevId()+" "+ns1.getUpdatedSuccId());
			System.out.println("Successor node :"+succid+" "+ns.getUpdatedPrevId()+" "+ns.getUpdatedSuccId());
			System.out.println("Prev node :"+previd+" "+nsPrev.getUpdatedPrevId()+" "+nsPrev.getUpdatedSuccId());		
			
			dos.writeUTF(servers.toString());
			dos.writeUTF(String.valueOf(firstKey));
			dos.writeUTF(String.valueOf(lastKey));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void exit(String[] cmd) {
		int nsid = Integer.parseInt(cmd[1]);
		//int succid = Integer.parseInt(cmd[2]);
		//int previd = Integer.parseInt(cmd[3]);
		
		//System.out.println(nsid +" "+succid);
		BootstrapServer.NS ns1 = BootstrapServer.cHashSystem.get(nsid);
		SortedMap<Integer, String> toAdd = ns1.getPairs();
		int succId=ns1.getUpdatedSuccId();
		int prevId=ns1.getUpdatedPrevId();
		System.out.println(nsid +" "+succId+" "+prevId);
		BootstrapServer.NS ns2 = BootstrapServer.cHashSystem.get(succId);
		SortedMap<Integer, String> addTo = ns2.getPairs();

		BootstrapServer.NS nsPrev = BootstrapServer.cHashSystem.get(prevId);
		nsPrev.setUpdatedSuccId(succId);
		
		ns2.setUpdatedPrevId(prevId);
		//int firstkey = previd+1 < toAdd.firstKey()? previd+1 : toAdd.firstKey() ;
		//int lastkey = nsid > toAdd.lastKey()? nsid : toAdd.lastKey();
		String range = ns1.firstKey + "-" + ns1.lastKey;
		for (int k : toAdd.keySet()) {
			addTo.put(k, toAdd.get(k));
		}

		ns2.setPairs(addTo);
		if(succId!=prevId) {
			ns2.setFirstKey(ns1.firstKey);
		}else {
			ns2.setFirstKey(0);
			ns2.setLastKey(1023);
		}
		
		BootstrapServer.cHashSystem.remove(nsid);

		try {
			dos.writeUTF("Successful exit");
			dos.writeUTF(String.valueOf(succId));
			dos.writeUTF(range);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}