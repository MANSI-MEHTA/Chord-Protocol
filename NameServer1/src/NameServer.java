import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class NameServer extends Thread {

	Socket nsSocket;
	DataInputStream dis;
	DataOutputStream dos;

	private int nameServerId;
	private int portNo;
	private String bsIp;
	private int bsPortNo;
	private int prevId, prevPort;
	private int sucId, sucPort;

	public NameServer(int nameServerId, int portNo, String bsIp, int bsPortNo) {
		this.nameServerId = nameServerId;
		this.portNo = portNo;
		this.bsIp = bsIp;
		this.bsPortNo = bsPortNo;
	}

	public int getNameServerId() {
		return nameServerId;
	}

	public int getPortNo() {
		return portNo;
	}

	public int getBsPortNo() {
		return bsPortNo;
	}

	public String getBsIp() {
		return bsIp;
	}

	public void setPrevId(int prevId) {
		this.prevId = prevId;
	}

	public void setPrevPort(int prevPort) {
		this.prevPort = prevPort;
	}

	public void setSucId(int sucId) {
		this.sucId = sucId;
	}

	public void setSucPort(int sucPort) {
		this.sucPort = sucPort;
	}

	public static void main(String[] args) {

		String configFileName = args[0];
		File configFile = new File(configFileName);

		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
			int nameServerId = Integer.parseInt(br.readLine());
			int portNo = Integer.parseInt(br.readLine());
			String[] bspair = br.readLine().split(" ");
			String bsIp = bspair[0];
			int bsPort = Integer.parseInt(bspair[1]);

			Thread t = new NameServer(nameServerId, portNo, bsIp, bsPort);
			t.start();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		String inputCmd = "";
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.print("Enter command:");
			inputCmd = scanner.nextLine();
			switch (inputCmd) {
			case "enter":
				enter();
				break;
			case "exit":
				exit();
				break;
			default:
				System.out.println("Invalid input command!");
				break;
			}
		}
	}
	
	public void enter() {
		try {
			String bsIp = getBsIp();
			int bsPortNo = getBsPortNo();
			nsSocket = new Socket(bsIp, bsPortNo);
			dis = new DataInputStream(nsSocket.getInputStream());
			dos = new DataOutputStream(nsSocket.getOutputStream());
			dos.writeUTF("enter " + String.valueOf(getNameServerId()));
			int prevId = Integer.parseInt(dis.readUTF());
			int prevPort = Integer.parseInt(dis.readUTF());
			int sucId = Integer.parseInt(dis.readUTF());
			int sucPort = Integer.parseInt(dis.readUTF());

			setPrevId(prevId);
			setPrevPort(prevPort);
			setSucId(sucId);
			setSucPort(sucPort);

			String servers = dis.readUTF();
			String fkey = dis.readUTF();
			String lastkey = dis.readUTF();
			System.out.println("Name server successfully added to the CH system");
			System.out.println("Range of keys managed by this server are:" + fkey + "-" + lastkey);
			System.out.println("Predecessor server id:" + prevId);
			System.out.println("Successor server id:" + sucId);
			System.out.println("Servers contacted to insert name server into CH system are:" + servers);

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void exit() {
		try {
			dos.writeUTF("exit " + String.valueOf(getNameServerId()) + " " + sucId+" "+prevId);
			String input;
			input = dis.readUTF();
			System.out.println(input);
			input = dis.readUTF();
			System.out.println("Successor id:" + input);
			input = dis.readUTF();
			System.out.println("Range handed over to successor:" + input);
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}