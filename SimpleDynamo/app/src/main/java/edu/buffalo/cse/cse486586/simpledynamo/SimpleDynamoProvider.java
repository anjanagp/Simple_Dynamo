package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	static String MY_PORT;
	Context myContext;


	ContentResolver mContentResolver;
	MatrixCursor myMatrixCursor;
	MatrixCursor myStarMatrixCursor;
	boolean SET = false;




	//List<String> mykeys_list = new ArrayList<String>();
	List<String> list1 = new ArrayList<String>(5);
	List<String> list2 = new ArrayList<String>(5);


	 /*
                * list : active list
                * list1 : port ids
                * list2 : corresponding hash values
                * Hashed value to port : list1.get(list2.indexOf(hash))
     */

	List<String> list_dead = new ArrayList<String>();




	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");;

	private final ContentValues mContentValues = new ContentValues();

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];

			try {
				Log.d("SERVER", "Entering ServerTask..");

				while (true) {

					Socket clientSocket = serverSocket.accept();

					Log.v("ABC SERVER", "Connection established..");

					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

					String message_Received = in.readLine();

					Log.v("ABC SERVER", "Received message on serverside");

					String[] message_received_split = message_Received.split("!");

					String message = message_received_split[0];

					String my_port = message_received_split[1];

					String key = message_received_split[2];

					Log.v("ABC ABC SERVER", "The key sent to server is " + key);

					String value_queried = null;

					if(message.equals("insert")){

						Log.v("ABC ABC INSERT", "Enters insert..");

						//insert the key and value

						String value = message_received_split[3];

						String i = message_received_split[4];

						Log.v("ABC ABC INSERT", "Key inserted is " + key + " and value is " + value);


						mContentValues.put("key", key);

						mContentValues.put("value", value);

						FileOutputStream outputStream;

						outputStream =  myContext.openFileOutput(key, Context.MODE_MULTI_PROCESS);//change mode

						outputStream.write(value.getBytes());

						outputStream.close();

						Log.v("ABC ABC SERVER", "Insert on serverside complete..");

						out.write("ACK-OK" + "\n");

						out.flush();

						Log.v("ABC ABC SERVER", "Ack sent from serverside..");




					}

					else if(message.equals("query")){

						Log.v("ABC SERVER", "Enters query on serverside..");

						BufferedReader in_q = new BufferedReader(new InputStreamReader(myContext.openFileInput(key)));

						value_queried = in_q.readLine();

						Log.v("ABC SERVER", "The value queried with sent key is " + value_queried);

						out.write(value_queried + "\n");

						out.flush();


					}

					else if(message.equals("queryStar")) {

						Log.v("ABC SERVER", "Entering queryStar part in Servertask..");

						StringBuilder stringbuild = new StringBuilder();
						stringbuild.append("querystarapp");

						String path = getContext().getFilesDir().getPath();
						File dir = new File(path);
						File[] directoryListing = dir.listFiles();
						if (directoryListing != null) {
							for (File child : directoryListing) {
								BufferedReader inflow = new BufferedReader(new InputStreamReader(myContext.openFileInput(child.getName())));
								String string_readcontent = inflow.readLine();
								stringbuild.append("!");
								stringbuild.append(child.getName());
								stringbuild.append("#");
								stringbuild.append(string_readcontent);


							}
						}

						String finalString = stringbuild.toString();

						//Log.d("ABC SERVER", "Finalstring " + finalString);

						out.write(finalString + "\n");

						out.flush();


					}

					else if(message.equals("list_dead")){
						//send deadlist to just woken up avd

						//out.write(list_dead + "\n");

						StringBuilder stringbuild = new StringBuilder();

						if(!list_dead.isEmpty()) {
							stringbuild.append(list_dead.get(0));

							for (int k = 1; k < list_dead.size(); k++) {

								Log.v("ABC INSERT", "List elements are:" + k);
								stringbuild.append("#");
								stringbuild.append(list_dead.get(k));
							}

							String finalDeadList = stringbuild.toString();

							out.write(finalDeadList+ "\n");

							out.flush();
						}
						else
						{
							out.write("Empty" + "\n");

							out.flush();
						}





						list_dead.clear();
					}



					}
			} catch (IOException e) {

				Log.e("ABC Server", "Servertask socket IOException");

			}


			return null;
		}

	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		protected Void doInBackground(String... msgs) {
			String message = msgs[0];

			String[] message_split = message.split("!");

			String header = message_split[0];

			String my_port = message_split[1];

			String key = message_split[2];

			//try{

				Log.v("ABC CLIENT", "Entering client task..");

				if(header.equals("insert")){

					String value = message_split[3];

					String[] remotePortArray = {message_split[4], message_split[5], message_split[6]};

					for(int i = 0; i < 3; i++) {

						Log.v("ABC CLIENT", "Entering client for loop " + i + " time");

						try{

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePortArray[i]));

						Log.v("ABC CLIENT", "Opened socket with " + remotePortArray[i]);

						BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));

						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						out.write(header + "!" + message_split[1] + "!" + key + "!" + value + "!" + i + "\n");

						Log.v("ABC CLIENT", "Message sent from client task");

						out.flush();

						String ack = in.readLine();

						if (ack == null) {
							//there has been a failure. store this key-value pair in deadlist

							String key_value_string = key + "!" + value;

							list_dead.add(key_value_string);


						}


						socket.close();
					}catch (Exception e) {
						Log.e("ABC Client", "ClientTask UnknownHostException ");
						if(header == "insert") {
							String key_value_string = message_split[2] + "!" + message_split[3];
							list_dead.add(key_value_string);


						}
						}

					}
				}

				else if(header.equals("query")){

					Log.v("ABC CLIENT", "Entering query in client");

					String node1 = message_split[3];

					String node2 = message_split[4];

					try {

						Socket socket_query = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node1));

						Log.v("ABC CLIENT", "Opened socket with " + node1);

						BufferedWriter out_query = new BufferedWriter(new OutputStreamWriter(socket_query.getOutputStream(), "UTF-8"));

						BufferedReader in_query = new BufferedReader(new InputStreamReader(socket_query.getInputStream()));

						out_query.write(header + "!" + my_port + "!" + key + "\n");

						out_query.flush();

						Log.v("ABC CLIENT", "Sent key to server ");

						//receive key and value and add to mymatrixcursor

						String value_received = in_query.readLine();

						//if node1 has failed ask node2
						if (value_received == null) {
							//failure_occured = true;

							Socket socket_query_second = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(node2));

							Log.v("ABC CLIENT", "Opened socket with " + node1);

							BufferedWriter out_query_sec = new BufferedWriter(new OutputStreamWriter(socket_query_second.getOutputStream(), "UTF-8"));

							BufferedReader in_query_sec = new BufferedReader(new InputStreamReader(socket_query_second.getInputStream()));

							out_query_sec.write(header + "!" + my_port + "!" + key + "\n");

							out_query_sec.flush();

							Log.v("ABC CLIENT", "Sent key to server ");

							//receive key and value and add to mymatrixcursor

							String value_received_sec = in_query_sec.readLine();

							value_received = value_received_sec;

						}

						Log.v("ABC CLIENT", "Value received by client " + value_received);

						myMatrixCursor.addRow(new Object[]{key, value_received});

						socket_query.close();
					}catch (Exception e) {
						Log.e("ABC Client", "ClientTask UnknownHostException ");

					}


				}

				else if(header.equals("star_query")) {

					for(int j = 0; j < 5; j++) {

						Log.v("ABC CLIENT", "Entering client star_query for loop " + j + " time");

						int port = Integer.parseInt(list1.get(j)) * 2;

						String port_str = Integer.toString(port);

						try {

							Socket socket_query_star = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port_str));

							Log.v("ABC CLIENT", "Opened socket with " + port_str);

							BufferedWriter out_query_star = new BufferedWriter(new OutputStreamWriter(socket_query_star.getOutputStream(), "UTF-8"));

							BufferedReader in_query_star = new BufferedReader(new InputStreamReader(socket_query_star.getInputStream()));

							out_query_star.write("queryStar" + "!" + MY_PORT + "!" + "1" + "\n");

							out_query_star.flush();

							String received_matrix_objects = in_query_star.readLine();

							Log.v("ABC CLIENT", "RECEIVED_MATRIX_OBJECTS " + received_matrix_objects);

							//need to split
							String[] matrix_each_row = received_matrix_objects.split("!");
							String[] key_value_each_row;

							for (int i = 1; i < matrix_each_row.length; i++) {

								Log.v("ABC CLIENT", "Entering loop to iterate matrix received..");

								key_value_each_row = matrix_each_row[i].split("#");
								String key_each_row = key_value_each_row[0];
								String val_each_row = key_value_each_row[1];

								myStarMatrixCursor.addRow(new Object[]{key_each_row, val_each_row});
							}

							socket_query_star.close();
						}catch (Exception e) {
							Log.e("ABC Client", "ClientTask UnknownHostException ");

						}

					}


				}

				else if(header.equals("list_dead")) {

					for (int k = 0; k < 5; k++) {

						Log.v("ABC CLIENT", "Entering client  for loop " + k + " time");

						int port_list_dead = Integer.parseInt(list1.get(k)) * 2;

						String port_str_list = Integer.toString(port_list_dead);

						try {

							Socket socket_list_dead = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port_str_list));

							Log.v("ABC CLIENT", "Opened socket with " + port_str_list);

							BufferedWriter out_list_dead = new BufferedWriter(new OutputStreamWriter(socket_list_dead.getOutputStream(), "UTF-8"));

							BufferedReader in_list_dead = new BufferedReader(new InputStreamReader(socket_list_dead.getInputStream()));

							out_list_dead.write(header + "!" + my_port + "!" + "1" + "\n");

							out_list_dead.flush();

							String received_dead_list = in_list_dead.readLine();

							if(received_dead_list.equals("Empty")){

								socket_list_dead.close();

								continue;

							}

							//the received list to be inserted into its db

							Log.v("ABC CLIENT", "The received dead_list is " + received_dead_list);

							String[] each_pair = received_dead_list.split("#");

							//go through each_pair array and add key-value
							for (String z : each_pair) {
								String[] key_value = z.split("!");

								String key_z = key_value[0];
								String value_z = key_value[1];


								//mContentValues.put("key", key_z);

								//mContentValues.put("value", value_z);

								FileOutputStream outputStream;

								outputStream = myContext.openFileOutput(key_z, Context.MODE_MULTI_PROCESS);//change mode

								outputStream.write(value_z.getBytes());

								outputStream.close();

							}

							socket_list_dead.close();
						}catch (Exception e) {
							Log.e("ABC Client", "ClientTask UnknownHostException ");


						}

					}

				}




//				}
//				catch (Exception e) {
//				Log.e("ABC Client", "ClientTask UnknownHostException ");
//				if(header == "insert") {
//
//
//					String key_value_string = message_split[2] + "!" + message_split[3];
//
//					list_dead.add(key_value_string);
//				}
//
//
//			}


			return null;
		}

	}


	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")) {
			File dir = getContext().getFilesDir();
			File file = new File(dir, selection);
			file.delete();
		}

		else if(selection.equals("*")){

		}

		else {
			File dir = getContext().getFilesDir();
			File file = new File(dir, selection);
			file.delete();

		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String filename = values.get("key").toString();

		String string = values.get("value").toString();

		try {

			Log.e("INSERT", "Entering insert function..");

			String filename_hashed = genHash(filename);

			Log.v("ABC INSERT", "The key value hashed is : " + filename_hashed);

			int my_Port = (Integer.parseInt(MY_PORT)) /2;

			String my_Port_string = Integer.toString(my_Port);

			String my_Port_hashed = list2.get(list1.indexOf(my_Port_string));

			//check hashed filename with list2. the two ports greater than it have to be sent the key and value
			List<String> list = new ArrayList<String>();

			list.clear();

			list.addAll(list2);

			list.add(filename_hashed);

			Collections.sort(list);

			for(String i : list){
				Log.v("ABC INSERT", "List elements are:" + i);
			}

			//check where filename hashed is in list. next 3 elements
			int key_index = list.indexOf(filename_hashed);

			Log.v("ABC INSERT", "The key's index in sorted list is " + key_index);

			Log.v("ABC INSERT", "The list size is " + list.size());

			int insertnode_index = (key_index + 1) % (list.size());

			int successor1_index = (insertnode_index + 1) % (list.size());

			int successor2_index = (successor1_index + 1) % (list.size());

			Log.v("ABC INSERT", "insertnode_index " + insertnode_index + ": " + "successor1_index " + successor1_index + ": " + "successor2_index " + successor2_index + "\n");

			//hash values
			String insertnode = list.get(insertnode_index);

			String successor1 = list.get(successor1_index);

			String successor2 = list.get(successor2_index);

			//get respective port ids from list1
			//Hashed value to port : list1.get(list2.indexOf(hash))
			int node1 = Integer.parseInt(list1.get(list2.indexOf(insertnode))) * 2;

			int node2 = Integer.parseInt(list1.get(list2.indexOf(successor1))) * 2;

			int node3 = Integer.parseInt(list1.get(list2.indexOf(successor2))) * 2;

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert" + "!" + MY_PORT + "!" + filename + "!" + string + "!" + node1 + "!" + node2 + "!" + node3);

			//Thread.sleep(500);

			list.clear();



		}catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		} //catch (Exception e){
			//Log.e("INSERT", "Exception in insert");
		//}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		Log.v("ABC ON_CREATE", "ENTERS ON CREATE..");


		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);

		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));

		int my_port_int = (Integer.parseInt(MY_PORT)) /2;

		Log.e("MYPORT","myport: " + my_port_int);

		list1.clear();
		list2.clear();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}catch(IOException e){

			Log.e("OnCreate", "Can't create a ServerSocket");
		}

		myContext = getContext();

		mContentResolver = myContext.getContentResolver();

		list1.add("5554");
		list1.add("5556");
		list1.add("5558");
		list1.add("5560");
		list1.add("5562");

		for(String in: list1) {
			Log.v("ABC ON_CREATE", "list1 is " + in);
		}

		try{
			list2.add(genHash(list1.get(0)));
			list2.add(genHash(list1.get(1)));
			list2.add(genHash(list1.get(2)));
			list2.add(genHash(list1.get(3)));
			list2.add(genHash(list1.get(4)));

			for(String in2: list2) {

				Log.v("ABC ON_CREATE", "list2 is " + in2);
			}


		}catch(NoSuchAlgorithmException e){

		}

		//if(FIRST_TIME == false){
			//Log.v("ABC ON_CREATE", "Enters firsttime false and asks for dead list..");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "list_dead" + "!" + MY_PORT + "!" + "1");

		//}
		//Once the avd is back on, it asks all the avds for their deadlists for the avd that failed and adds all these key-values to its matrixcursor
		//FIRST_TIME = false;

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String string_readcontent;

		while(SET == true){

		}
		SET = true;

		myMatrixCursor = null;
		myMatrixCursor = new MatrixCursor(new String[] {"key", "value"});

		myStarMatrixCursor = null;
		myStarMatrixCursor = new MatrixCursor(new String[] {"key", "value"});

		try {

			Log.v("ABC QUERY", "Entering query..");

			Log.e("QUERY", "The key to be queried is " + selection);

			String selection_hashed = genHash(selection);


			if (selection.equals("@")) {
				Log.v("ABC QUERY", "Enters @ selection in query..");
				String path = getContext().getFilesDir().getPath();
				File dir = new File(path);
				File[] directoryListing = dir.listFiles();
				if (directoryListing != null) {
					for (File child : directoryListing) {
						BufferedReader in = new BufferedReader(new InputStreamReader(myContext.openFileInput(child.getName())));

						string_readcontent = in.readLine();
						myMatrixCursor.addRow(new Object[]{child.getName(), string_readcontent});
					}
				}

				Thread.sleep(500);

				SET = false;

				return myMatrixCursor;
			}

			else if(selection.equals("*")){

				//MatrixCursor myLocalMatrixCursor = null;
				//myLocalMatrixCursor = new MatrixCursor(new String[] {"key", "value"});



				Log.v("ABC QUERY", "Enters * selection in query..");
				/*String path = getContext().getFilesDir().getPath();
				File dir = new File(path);
				File[] directoryListing = dir.listFiles();
				if (directoryListing != null) {
					for (File child : directoryListing) {
						BufferedReader in = new BufferedReader(new InputStreamReader(myContext.openFileInput(child.getName())));
						string_readcontent = in.readLine();
						myStarMatrixCursor.addRow(new Object[]{child.getName(), string_readcontent});
					}

				}
				Log.v("ABC QUERY", "Finished populating its own matrixcursor..");*/

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "star_query" + "!" + MY_PORT + "!" + "1");

				Thread.sleep(500);

				SET = false;

				return myStarMatrixCursor;


			}

			else{

				//create local matrixcursor

				Log.v("ABC QUERY", "Enters selection in query..");

				List<String> list_order = new ArrayList<String>();

				list_order.clear();

				list_order.addAll(list2);

				list_order.add(selection_hashed);

				Collections.sort(list_order);

				for(String i : list_order){

					Log.v("ABC QUERY", "List elements are:" + i);
				}

				int selection_index = list_order.indexOf(selection_hashed);

				int insertnode_index = (selection_index + 1) % (list_order.size());

				Log.v("ABC QUERY", "The insertnode_index is " + insertnode_index);

				String insertnode = list_order.get(insertnode_index);

				Log.v("ABC QUERY", "The insertnode_in list is " + insertnode);

				//first successor
				int node1 = Integer.parseInt(list1.get(list2.indexOf(insertnode))) * 2;

				String node1_string = Integer.toString(node1);

				Log.v("ABC QUERY", "Insert node is " + node1_string);

				//second successor

				int insertnode_index_second = (insertnode_index + 1) % (list_order.size());

				Log.v("ABC QUERY", "The insertnode_index is " + insertnode_index);

				String insertnode_second = list_order.get(insertnode_index_second);

				Log.v("ABC QUERY", "The insertnode_in list is " + insertnode);

				int node2 = Integer.parseInt(list1.get(list2.indexOf(insertnode_second))) * 2;

				String node2_string = Integer.toString(node2);

				Log.v("ABC QUERY", "Insert second node is " + node2_string);

				//in case of failure will have to contact the next one

				// if node1 is myport look within my contentresolver itself else spawn clienttask

				if(node1_string.equals(MY_PORT)){

					Log.v("ABC QUERY", "My own port ");

					BufferedReader in = new BufferedReader(new InputStreamReader(myContext.openFileInput(selection)));

					string_readcontent = in.readLine();

					myMatrixCursor.addRow(new Object[]{selection, string_readcontent});

				}
				else{

					//Log.v("ABC QUERY", "Ask other ports ");

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query" + "!" + MY_PORT + "!" + selection + "!" + node1 + "!" + node2);


				}

				Thread.sleep(500);
				MatrixCursor myLocalMatrixCursor= myMatrixCursor;

				SET = false;

				return myLocalMatrixCursor;

			}



		}catch (Exception e){
			Log.e("QUERY", "File not found");

		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
