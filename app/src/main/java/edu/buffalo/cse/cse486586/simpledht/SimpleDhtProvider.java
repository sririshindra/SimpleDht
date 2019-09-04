package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private Uri providerUri;
    static final String[] REMOTE_PORT = new String[]{"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static String my_port = null;
    static String nodeId = null;
    static String successorID = null;
    static String predecessorId = null;
    static String successorPortStr = null;
    static String predecessorPortStr = null;
    static List<String> listOfNodesInChord = null;
    static Map<String, String> hashMap = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i(TAG, "delete is called ");
        String message = "delete" + "_" + selection + "_" + my_port;
        if (selection.equalsIgnoreCase("@")) {
            hashMap.clear();
        } else if (selection.equalsIgnoreCase("*")) {
            hashMap.clear();
            forwardMessageToSuccessor(message);
        } else if (isMessageAddressedToMe(message)) {
            deleteMessageFromMyHashMap(message);
        } else {
            forwardMessageToSuccessor(message);
        }
        return 0;
    }

    private boolean isMessageAddressedToMe(String message) {
        String messageToBeCompared = null;
        try {
            String[] messageArray = message.split("_");
            messageToBeCompared = genHash(messageArray[1]);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (predecessorId.equalsIgnoreCase(nodeId)) {
            return true;
        }
        Log.i(TAG, "message in isMessageAddressedToMe is " + message);
        Log.i(TAG, "messageToBeCompared is " + messageToBeCompared);
        Log.i(TAG, "predecessorId is " + predecessorId);
        Log.i(TAG, "nodeId is " + nodeId);


        if (nodeId.compareTo(predecessorId) < 0) {
            Log.i(TAG, "entered first if ");
            if (messageToBeCompared.compareTo(predecessorId) > 0 ||
                    messageToBeCompared.compareTo(nodeId) < 0) {
                Log.i(TAG, "entered second if ");
                return true;
            } else {
                Log.i(TAG, "entered first else ");
                return false;
            }
        } else {
            Log.i(TAG, "entered second else ");
            if (messageToBeCompared.compareTo(predecessorId) > 0) {
                Log.i(TAG, "entered third if ");
                if (messageToBeCompared.compareTo(nodeId) < 0) {
                    Log.i(TAG, "entered fourth if ");
                    return true;
                } else {
                    Log.i(TAG, "entered third else ");
                    return false;
                }
            } else {
                Log.i(TAG, "entered fourth else ");
                return false;
            }
        }

//        if (messageToBeCompared.compareTo(predecessorId) > 0 ) {
//            Log.i(TAG, "entered if in isMessageAddressedToMe ");
//            if (messageToBeCompared.compareTo(successorID) > 0 ){
//                Log.i(TAG, "entered second if in isMessageAddressedToMe ");
//                return true;
//            } else {
//                if (messageToBeCompared.compareTo(nodeId) > 0 ){
//                    Log.i(TAG, "entered third if in isMessageAddressedToMe ");
//                    return false;
//                } else {
//                    Log.i(TAG, "entered first else in isMessageAddressedToMe ");
//                    return true;
//                }
//            }
//
//        } else {
//            Log.i(TAG, "entered second else in isMessageAddressedToMe ");
//            return false;
//        }

    }

    private void deleteMessageFromMyHashMap(String message) {
        String[] msgArray = message.split("_");
        String msgToBeDeleted = msgArray[1];

        hashMap.remove(msgToBeDeleted);

    }

    private String forwardMessageToSuccessor(String message) {

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successorPortStr) * 2);


            PrintWriter pwOutStream = new PrintWriter(socket.getOutputStream(), true);
            pwOutStream.println(message);
            BufferedReader brInStream = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            String response = brInStream.readLine();

            pwOutStream.close();
            brInStream.close();
            socket.close();
            return response;

        } catch (IOException e) {
            Log.e(TAG, "----> an error occurred in forward message to Successor for");
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        Log.i(TAG, "insert is called " + values);
        String message = "insert" + "_" + values.get("key") + "_" + values.get("value");
        if (isMessageAddressedToMe(message)) {
            Log.i(TAG, "message is addressed to me for the key " + values.get("key"));
            insertMessageInHashMap(message);
        } else {
            Log.i(TAG, "message is not addressed to me for the key " + values.get("key")
                    + " and is forwarded");
            forwardMessageToSuccessor(message);
        }
        return null;
    }

    private void insertMessageInHashMap(String message) {
        String[] msgArray = message.split("_");
        String key = msgArray[1];
        String value = msgArray[2];
        hashMap.put(key, value);

    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        my_port = portStr;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        try {
            nodeId = genHash(portStr);
            Log.i(TAG, "nodeId is " + nodeId);
            Log.i(TAG, "On Create  is called on " + portStr
                    + " and the corresponding NodeId is " + nodeId);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.i(TAG, "portStr is " + portStr);
        if (portStr.equalsIgnoreCase("5554")) {
            predecessorId = nodeId;
            successorID = nodeId;
            predecessorPortStr = "5554";
            successorPortStr = "5554";
            listOfNodesInChord = new LinkedList<String>();
            try {
                listOfNodesInChord.add("5554" + "_" + genHash("5554"));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        } else {
            Log.i(TAG, "portStr is " + portStr);
            String PredecessorSuccessor = null;
            try {
                PredecessorSuccessor = new FindPredecessorAndSuccessor().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Log.i(TAG, "PredecessorSuccessor is " + PredecessorSuccessor);

            if(PredecessorSuccessor != null){
                String[] PredecessorSuccessorArray = PredecessorSuccessor.split("_");
                predecessorPortStr = PredecessorSuccessorArray[0];
                predecessorId = PredecessorSuccessorArray[1];
                successorPortStr = PredecessorSuccessorArray[2];
                successorID = PredecessorSuccessorArray[3];

                try {
                    new UpdateNodePointers().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "setPredecessor", successorPortStr, successorID).get();
//                new UpdateNodePointers().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "setPredecessor", successorPortStr, successorID);
//                new UpdateNodePointers().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "setSuccessor", predecessorPortStr, predecessorId);
                    new UpdateNodePointers().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "setSuccessor", predecessorPortStr, predecessorId).get();
                    String returnedMsg = new TakeMyKeyValuePairsFromSuccessor().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
                    Log.i(TAG, "returnedMsg is " + returnedMsg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            } else {
                predecessorId = nodeId;
                successorID = nodeId;
                predecessorPortStr = portStr;
                successorPortStr = portStr;
            }

        }


        return true;
    }


    private void addKeyValuesToHashMap(List<String> keyVales) {

        Iterator<String> iter = keyVales.iterator();

        while (iter.hasNext()) {
            String[] keyValue = iter.next().split(":");
            String key = keyValue[0];
            String value = keyValue[1];

            hashMap.put(key, value);
        }

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String message = "query" + "_" + selection + "_" + my_port;

        if (selection.equalsIgnoreCase("@")){
            Iterator<String> iter = hashMap.keySet().iterator();
            while (iter.hasNext()){
                String key = iter.next();
                String value = hashMap.get(key);
                matrixCursor.addRow(new String[]{key, value});
            }

        } else if (selection.equalsIgnoreCase("*")){
            String response = forwardMessageToSuccessor(message);
            try {
                JSONObject keyValuePairs = new JSONObject(response);
//                JSONObject tempKeyValuePairs = new JSONObject();
                Iterator keyValuePairsIter = keyValuePairs.keys();
                while (keyValuePairsIter.hasNext()){
                    String key = keyValuePairsIter.next().toString();
                    matrixCursor.addRow(new String[]{key, keyValuePairs.getString(key)});
                }
                Iterator<String> hashMapIter = hashMap.keySet().iterator();
                while (hashMapIter.hasNext()){
                    String key = hashMapIter.next().toString();
                    matrixCursor.addRow(new String[]{key, hashMap.get(key)});

                }

            } catch (JSONException e) {
                e.printStackTrace();
            }

        } else {

            String msgInHashMap = null;
            if (isMessageAddressedToMe(message)) {
                Log.i(TAG, "retrevied message from my hashmap");
                msgInHashMap = getMessageInMyHashMap(message);
            } else {
                Log.i(TAG, "retrevied message from sucessor ");
                msgInHashMap = forwardMessageToSuccessor(message);
            }
            Log.i(TAG, "query is called and msgInHashMap in hash map is " + msgInHashMap);
            matrixCursor.addRow(new String[]{selection, msgInHashMap});
        }

        return matrixCursor;
    }

    private String getMessageInMyHashMap(String message) {

        String key = message.split("_")[1].split(":")[0];
        return hashMap.get(key);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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


    private class FindPredecessorAndSuccessor extends AsyncTask<String, Void, String> {
        protected String doInBackground(String... msgs) {

            Socket socket;
            try {
                Log.i(TAG, "try in FindPredecessorAndSuccessor is called");
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("5554") * 2);
                PrintWriter pwOutStream = new PrintWriter(socket.getOutputStream(), true);
                pwOutStream.println("PredecessorAndSuccessor_" + my_port + "_" + nodeId);
                BufferedReader brInStream = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                String predecessorAndSuccessor;
                while ((predecessorAndSuccessor = brInStream.readLine()) != null) {
                    Log.i(TAG, "predecessorAndSuccessor in try in FindPredecessorAndSuccessor is "
                            + predecessorAndSuccessor);
                    break;
                }
                return predecessorAndSuccessor;

            } catch (IOException e) {
                Log.e(TAG, "an IOException in findPredecessorAndSuccessor ");
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "an Exception in findPredecessorAndSuccessor ");
                e.printStackTrace();
            }


            return null;
        }
    }

    private class UpdateNodePointers extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... msgs) {
            String startMessage = msgs[0];
            String portStr = msgs[1];


            try {
                Log.i(TAG, "UpdateNodePointers is called and portStr is " + portStr
                        + " and startMessage is " + startMessage);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portStr) * 2);

                PrintWriter pwOutStream = new PrintWriter(socket.getOutputStream(), true);
                pwOutStream.println(startMessage + "_" + my_port + "_" + nodeId);
                BufferedReader brInStream = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));

                String response = brInStream.readLine();
                Log.i(TAG, "updated pointers for  " + startMessage + " " + response);
                pwOutStream.close();
                brInStream.close();
                socket.close();
                return "response";

            } catch (IOException e) {
                Log.e(TAG, "----> an error occurred in forward message to Successor for");
                e.printStackTrace();
            }
            return null;
        }
    }

    private class TakeMyKeyValuePairsFromSuccessor extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            Socket socket;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(successorPortStr) * 2);
                PrintWriter pwOutStream = new PrintWriter(socket.getOutputStream(), true);
                pwOutStream.println("giveKeyValuePairsFromSuccessor_" + nodeId);
                BufferedReader brInStream = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                String message = brInStream.readLine();

                JSONObject keyValuePairs = new JSONObject(message);
                Log.i(TAG, "keyValuePairs received is " + keyValuePairs.toString());
                Iterator keyValuePairsIter = keyValuePairs.keys();
                Log.i(TAG, "Size of hashmap before adding received keys " + hashMap.size());
                while (keyValuePairsIter.hasNext()) {
                    String key = keyValuePairsIter.next().toString();
                    hashMap.put(key, keyValuePairs.getString(key));
                }
                Log.i(TAG, "Size of hashmap after adding received keys " + hashMap.size());
            } catch (IOException e) {
                Log.e(TAG, "IO Exception occurred in method TakeMyKeyValuePairsFromSuccessor");
                e.printStackTrace();
            } catch (JSONException e) {
                Log.e(TAG, "JSONException occurred in method TakeMyKeyValuePairsFromSuccessor");
                e.printStackTrace();
            }

            return "took key value pairs from successor";
        }

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {

                String message;
                //to accept messages continuously
                while (true) {
                    Socket client_socket = serverSocket.accept();
                    Log.i(TAG, " accepted client_socket is " + client_socket);
                    BufferedReader brInStream = new BufferedReader(
                            new InputStreamReader(client_socket.getInputStream()));
                    Log.i(TAG, "socket status is " + client_socket.isConnected());
                    if ((message = brInStream.readLine()) != null) {
                        Log.i(TAG, "received message from some client  and the message is "
                                + message);
                        if (message.startsWith("PredecessorAndSuccessor")) {
                            givePredecessorAndSuccessor(client_socket, message);
                        } else if (message.startsWith("giveKeyValuePairsFromSuccessor")) {
                            giveKeyValuePairsToPredecessor(client_socket, message);
                            removeMessagesOfPredecessorFromMyHashMap();
                        } else if (message.startsWith("setPredecessor")) {
                            String[] messageArray = message.split("_");
                            predecessorPortStr = messageArray[1];
                            predecessorId = messageArray[2];
                            sendAckToClient(client_socket, "pointers_set");
                        } else if (message.startsWith("setSuccessor")) {
                            String[] messageArray = message.split("_");
                            successorPortStr = messageArray[1];
                            successorID = messageArray[2];
                            sendAckToClient(client_socket, "pointers_set");
                        } else if (message.startsWith("insert") || message.startsWith("query")
                                || message.startsWith("delete")) {
                            String[] messageArray = message.split("_");

                            if (messageArray[1].equalsIgnoreCase("*")){

                                if(successorPortStr.equalsIgnoreCase(messageArray[2])){
                                    if (message.startsWith("delete")){
                                        hashMap.clear();
                                        sendAckToClient(client_socket, "all mesasges deleted");
                                    } else {
                                        JSONObject tempKeyValuePairs = new JSONObject();
                                        Iterator<String> hashMapIter = hashMap.keySet().iterator();
                                        while (hashMapIter.hasNext()){
                                            String key = hashMapIter.next().toString();
                                            tempKeyValuePairs.put(key,
                                                    hashMap.get(key));
                                        }
                                        sendAckToClient(client_socket, tempKeyValuePairs.toString());
                                    }
                                } else {
                                    String response = forwardMessageToSuccessor(message);
                                    if (message.startsWith("delete")){
                                        hashMap.clear();
                                        sendAckToClient(client_socket, "all mesasges deleted");
                                    } else {
                                        JSONObject keyValuePairs = new JSONObject(response);
                                        JSONObject tempKeyValuePairs = new JSONObject();
                                        Iterator keyValuePairsIter = keyValuePairs.keys();
                                        while (keyValuePairsIter.hasNext()){
                                            String key = keyValuePairsIter.next().toString();
                                            tempKeyValuePairs.put(key,
                                                    keyValuePairs.getString(key));
                                        }
                                        Iterator<String> hashMapIter = hashMap.keySet().iterator();
                                        while (hashMapIter.hasNext()){
                                            String key = hashMapIter.next().toString();
                                            tempKeyValuePairs.put(key,
                                                    hashMap.get(key));
                                        }
                                        sendAckToClient(client_socket, tempKeyValuePairs.toString());
                                    }
                                }

                            } else {
                                if (isMessageAddressedToMe(message)) {
                                    if (message.startsWith("insert")) {
                                        insertMessageInHashMap(message);
                                        sendAckToClient(client_socket, "msg_inserted");
                                    } else if (message.startsWith("query")) {
                                        String msgInMap = getMessageInMyHashMap(message);
                                        sendAckToClient(client_socket, msgInMap);
                                    } else if (message.startsWith("delete")) {
                                        deleteMessageFromMyHashMap(message);
                                        sendAckToClient(client_socket, "msg_deleted");
                                    }

                                } else {
                                    String response = forwardMessageToSuccessor(message);
                                    sendAckToClient(client_socket, response);
                                }
                            }


                        }

                    }
                    client_socket.close();

                }
            } catch (IOException e) {
                Log.e(TAG, "Server socket IOException ");
                e.printStackTrace();
            } catch (JSONException e) {
                Log.e(TAG, "Server JSONException ");
                e.printStackTrace();
            }

            return null;
        }

        private void giveKeyValuePairsToPredecessor(Socket client_socket, String message) {

            Log.i(TAG, "giveKeyValuePairsToPredecessor is called for node " + my_port
                    + " and the message is " + message);

            JSONObject keyValuePairs = new JSONObject();

            Log.i(TAG, "size of HashMap is " + hashMap.size());
            Iterator<Map.Entry<String, String>> iter = hashMap.entrySet().iterator();
            while (iter.hasNext()) {
                try {
                    Log.i(TAG, "Entered While loop in line 480");
                    Map.Entry<String, String> keyValue = iter.next();
                    Log.i(TAG, "key is " + keyValue.getKey() + "hash of keyValue is " + genHash(keyValue.getKey()) + " value is "
                            + keyValue.getValue() + " predecessorId is " + predecessorId);

                    String constructed_message = "constructedMessage_" + keyValue.getKey();

//                    if (genHash(keyValue.getKey()).compareTo(predecessorId) < 0) {
                    if (!isMessageAddressedToMe(constructed_message)) {
                        try {
                            keyValuePairs.put(keyValue.getKey(), keyValue.getValue());
                        } catch (JSONException e) {
                            Log.e(TAG, "JSONException occurred in method giveKeyValuePairsToPredecessor");
                            e.printStackTrace();
                        }


                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            Log.i(TAG, "keyValuePairs sent is " + keyValuePairs.toString());
            sendAckToClient(client_socket, keyValuePairs.toString());

        }

        private void removeMessagesOfPredecessorFromMyHashMap() {

            Log.i(TAG, "removeMessagesOfPredecessorFromMyHashMap method is called and " +
                    "predecessorId is " + predecessorId + " and nodeId is " + nodeId);

            Log.i(TAG, "Size of HashMap is before removing keys " + hashMap.size());
            Iterator<String> iter = hashMap.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                try {
                    String constructedMessage = "constructedMessage_" + key;
//                    if (genHash(key).compareTo(predecessorId) < 0) {
                    if (!isMessageAddressedToMe(constructedMessage)) {
                        iter.remove();

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Log.i(TAG, "Size of HashMap is after removing keys " + hashMap.size());
        }

        private void givePredecessorAndSuccessor(Socket client_socket, String message) {

            Log.i(TAG, "givePredecessorAndSuccessor is called on my_port " + my_port
                    + " and the message is " + message);
            String[] _port_hashId = message.split("_");
            int sizeOfNodesInChord = listOfNodesInChord.size();


            if (sizeOfNodesInChord == 1) {

                String hashIdToCompare = listOfNodesInChord.get(0).split("_")[1];
                if (hashIdToCompare.compareTo(_port_hashId[2]) < 0) {
                    listOfNodesInChord.add(_port_hashId[1] + "_" + _port_hashId[2]);
                    sendAckToClient(client_socket, listOfNodesInChord.get(0) + "_"
                            + listOfNodesInChord.get(0));
                } else {
                    listOfNodesInChord.add(0, _port_hashId[1] + "_" + _port_hashId[2]);
                    sendAckToClient(client_socket, listOfNodesInChord.get(1) + "_"
                            + listOfNodesInChord.get(1));
                }


            } else {
                for (int counter = 0; counter < sizeOfNodesInChord; counter++) {

                    String possiblePredecessor = listOfNodesInChord.get(counter);
                    String[] possiblePredecessor_port_hashId = possiblePredecessor.split("_");
                    String possiblePredecessorPort = possiblePredecessor_port_hashId[0];
                    String possiblePredecessorHashId = possiblePredecessor_port_hashId[1];

                    String predecessorString;
                    String successorString;
                    if (_port_hashId[2].compareTo(possiblePredecessorHashId) < 0) {
                        listOfNodesInChord.add(counter, _port_hashId[1] + "_" + _port_hashId[2]);

                        if (counter == 0) {
                            predecessorString = listOfNodesInChord.get(listOfNodesInChord.size() - 1);
                        } else {
                            predecessorString = listOfNodesInChord.get(counter - 1);
                        }
                        successorString = listOfNodesInChord.get(counter + 1);
                        sendAckToClient(client_socket, predecessorString + "_"
                                + successorString);
                        break;
                    } else {
                        if (counter + 1 == sizeOfNodesInChord) {
                            listOfNodesInChord.add(_port_hashId[1] + "_" + _port_hashId[2]);
                            predecessorString = listOfNodesInChord.get(listOfNodesInChord.size() - 2);
                            successorString = listOfNodesInChord.get(0);
                            sendAckToClient(client_socket, predecessorString + "_"
                                    + successorString);

                            break;
                        }

                    }


                }

            }

        }

        private void sendAckToClient(Socket client_socket, String ack) {
            PrintWriter pwOutStream;
            try {
                pwOutStream = new PrintWriter(client_socket.getOutputStream(), true);
                pwOutStream.println(ack);
                pwOutStream.close();
            } catch (IOException e) {
                Log.e(TAG, "failed to send ack to client for ack " + ack + " for client "
                        + client_socket);
                e.printStackTrace();
            }

        }

    }


}

