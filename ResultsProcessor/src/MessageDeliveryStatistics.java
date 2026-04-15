import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class MessageDeliveryStatistics {

	private class MessageInformation {

		public long transmissionTime;
		public long lastDeliveryTime;
		public int nodeCount;

		private MessageInformation() {
			this.transmissionTime = -1;
			this.lastDeliveryTime = -1;
			this.nodeCount = 0;
		}

		private void registerSendTime(long timestamp) {
			this.transmissionTime = timestamp;
		}

		private void registerDelivery(long timestamp) {
			this.nodeCount++;
			if (timestamp > lastDeliveryTime)
				lastDeliveryTime = timestamp;
		}

		private long getLantency() {
			return this.lastDeliveryTime - this.transmissionTime;
		}

		private int getReceivers() {
			return this.nodeCount;
		}
	}

	private Path rootDir;
	private HashMap<UUID, MessageInformation> data;
	private int nodeCount;

	public MessageDeliveryStatistics(Path rootDir) {
		this.rootDir = rootDir;
		this.data = new HashMap<UUID, MessageDeliveryStatistics.MessageInformation>();
		this.nodeCount = 0;
	}

	/**
	 * Processes distributed experiment logs to compute:
	 * - Average delivery rate per message (across all 5000 nodes)
	 * - Average latency per message (time from SEND to last RECV, in ms)
	 *
	 * Expected directory structure:
	 * <root>/
	 * 10.12.0.100/
	 * charmander-2-message.log
	 * 10.12.0.101/
	 * ...
	 *
	 * Log line format:
	 * <unix-ms> <date> <time> <node-ip:port> SEND|RECV <msg-id> <size>
	 *
	 * Message ID format: <sender-ip:port>::<uuid>
	 * 
	 * @throws IOException
	 */

	private void readLogFiles() throws IOException {
		if (!Files.isDirectory(rootDir)) {
			System.err.println("Usage: java -jar ResultsProcessor <root-log-dir>");
			System.exit(1);
		}

		System.err.println("Looking for log files...");
		List<Path> logFiles = new ArrayList<>();
		try (DirectoryStream<Path> nodeDirs = Files.newDirectoryStream(rootDir)) {
			for (Path nodeDir : nodeDirs) {
				if (!Files.isDirectory(nodeDir))
					continue;
				try (DirectoryStream<Path> files = Files.newDirectoryStream(nodeDir, "*-message.log")) {
					for (Path f : files) {
						logFiles.add(f);
					}
				}
			}
		}
		System.err.println("Found a totoal of " + logFiles.size() + " files");

		for (int processingFilesCounter = 1; processingFilesCounter <= logFiles.size(); processingFilesCounter++) {
			System.err.println("Processing file #" + processingFilesCounter + ": "
					+ logFiles.get(processingFilesCounter - 1).getFileName().toString());

			Scanner scan = new Scanner(logFiles.get(processingFilesCounter - 1).toFile());
			int line = 0;
			String l = null;
			while (scan.hasNextLine()) {
				try {
					l = scan.nextLine();

					if (!l.contains("RECV") && !l.contains("SEND"))
						continue;

					line++;

					String[] elements = l.split(" ");

					String rawId = elements[5];
					String uuidPart;

					if (rawId.contains("::")) {
						uuidPart = rawId.split("::")[1];
					} else {
						uuidPart = rawId;
					}

					UUID msgID = UUID.fromString(uuidPart);

					MessageInformation stats = data.get(msgID);
					if (stats == null) {
						stats = new MessageInformation();
						data.put(msgID, stats);
					}

					long timestamp = Long.parseLong(elements[0]);

					if (elements[4].equalsIgnoreCase("RECV")) {
						stats.registerDelivery(timestamp);
					} else if (elements[4].equalsIgnoreCase("SEND")) {
						stats.registerSendTime(timestamp);
					} else {
						System.err.println("Malformed line on log "
								+ logFiles.get(processingFilesCounter - 1).toString().toString() + " on line " + line);
						System.err.println("Line: " + l);
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Malformed line on log "
							+ logFiles.get(processingFilesCounter - 1).toString().toString() + " on line " + line);
					System.err.println("Line: " + l);
				}
				l = null;
			}
			scan.close();
			this.nodeCount++;
			System.err.println("Processed file #" + processingFilesCounter + ": "
					+ logFiles.get(processingFilesCounter - 1).getFileName().toString() + " (total of " + line
					+ " lines processed.)");
		}

		System.err.println("Processing log files complete (processed " + logFiles.size() + " files)");
	}

	double minLatency;
	double avgLatency;
	double maxLatency;
	double minReliability;
	double avgReliability;
	double maxReliability;

	int totalNumberOfMessage;

	private void computeStatistics() {

		this.totalNumberOfMessage = 0;
		this.avgLatency = 0;
		this.maxLatency = Double.MIN_VALUE;
		this.minLatency = Double.MAX_VALUE;
		this.avgReliability = 0;
		this.minReliability = Double.MAX_VALUE;
		this.maxReliability = Double.MIN_VALUE;

		for (MessageInformation info : data.values()) {

			totalNumberOfMessage++;
			long latency = info.getLantency();
			double reliability = (double) (((double) info.getReceivers()) / this.nodeCount);

			if (this.minLatency > latency) {
				this.minLatency = latency;
			}
			this.avgLatency += latency;
			if (this.maxLatency < latency) {
				this.maxLatency = latency;
			}

			if (this.minReliability > reliability) {
				this.minReliability = reliability;
			}
			this.avgReliability += reliability;
			if (this.maxReliability < reliability) {
				this.maxReliability = reliability;
			}
		}

		this.avgLatency = this.avgLatency / totalNumberOfMessage;
		this.avgReliability = this.avgReliability / totalNumberOfMessage;
	}

	private void generateOutput() {
		System.out.println("Total Messages: " + totalNumberOfMessage);
		System.out.println("Average Reliability: " + avgReliability * 100 + " %");
		System.out.println("Average Latency: " + avgLatency + " ms");
		System.out.println("");
		System.out.println("Minimum Reliability: " + minReliability * 100 + " %");
		System.out.println("Maximum Reliability: " + maxReliability * 100 + " %");
		System.out.println("");
		System.out.println("Minimum Latency: " + minLatency + " ms");
		System.out.println("Maximum Latency: " + maxLatency + " ms");
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Usage: java -jar ResultsProcessor <root-log-dir>");
			System.exit(1);
		}

		MessageDeliveryStatistics mds = new MessageDeliveryStatistics(Paths.get(args[0]));

		// Read Files
		System.err.println("Reading log lines");
		mds.readLogFiles();
		mds.computeStatistics();
		mds.generateOutput();
	}
}
