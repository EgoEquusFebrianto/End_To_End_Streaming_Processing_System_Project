package kudadiri.DataEngineer.portofolio.utility;

import kudadiri.kafka.serializer.DataRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CsvReaderService {
    public static List<DataRecord> readCsv(String filePath) {
        List<DataRecord> records = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//        int recordCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String headerLine = br.readLine();
            String[] headers = headerLine.split(",");

            Map<String, Integer> indexMap = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                indexMap.put(headers[i], i);
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] v = line.split(",", -1);
                DataRecord.Builder r = DataRecord.newBuilder();

                r.setUnnamed0(parseInt(v, indexMap, "Unnamed_0"));
                r.setFlowID(getString(v, indexMap, "Flow_ID"));
                r.setSourceIP(getString(v, indexMap, "Source_IP"));
                r.setSourcePort(parseInt(v, indexMap, "Source_Port"));
                r.setDestinationIP(getString(v, indexMap, "Destination_IP"));
                r.setDestinationPort(parseInt(v, indexMap, "Destination_Port"));
                r.setProtocol(parseInt(v, indexMap, "Protocol"));

                r.setTimestamp(parseTimestamp(v, indexMap, "Timestamp"));

                r.setFlowDuration(parseInt(v, indexMap, "Flow_Duration"));
                r.setTotalFwdPackets(parseInt(v, indexMap, "Total_Fwd_Packets"));
                r.setTotalBackwardPackets(parseInt(v, indexMap, "Total_Backward_Packets"));

                r.setTotalLengthOfFwdPackets(parseDouble(v, indexMap, "Total_Length_of_Fwd_Packets"));
                r.setTotalLengthOfBwdPackets(parseDouble(v, indexMap, "Total_Length_of_Bwd_Packets"));

                r.setFwdPacketLengthMax(parseDouble(v, indexMap, "Fwd_Packet_Length_Max"));
                r.setFwdPacketLengthMin(parseDouble(v, indexMap, "Fwd_Packet_Length_Min"));
                r.setFwdPacketLengthMean(parseDouble(v, indexMap, "Fwd_Packet_Length_Mean"));
                r.setFwdPacketLengthStd(parseDouble(v, indexMap, "Fwd_Packet_Length_Std"));

                r.setBwdPacketLengthMax(parseDouble(v, indexMap, "Bwd_Packet_Length_Max"));
                r.setBwdPacketLengthMin(parseDouble(v, indexMap, "Bwd_Packet_Length_Min"));
                r.setBwdPacketLengthMean(parseDouble(v, indexMap, "Bwd_Packet_Length_Mean"));
                r.setBwdPacketLengthStd(parseDouble(v, indexMap, "Bwd_Packet_Length_Std"));

                r.setFlowBytesS(parseDouble(v, indexMap, "Flow_Bytes_s"));
                r.setFlowPacketsS(parseDouble(v, indexMap, "Flow_Packets_s"));

                r.setFlowIATMean(parseDouble(v, indexMap, "Flow_IAT_Mean"));
                r.setFlowIATStd(parseDouble(v, indexMap, "Flow_IAT_Std"));
                r.setFlowIATMax(parseDouble(v, indexMap, "Flow_IAT_Max"));
                r.setFlowIATMin(parseDouble(v, indexMap, "Flow_IAT_Min"));

                r.setFwdIATTotal(parseDouble(v, indexMap, "Fwd_IAT_Total"));
                r.setFwdIATMean(parseDouble(v, indexMap, "Fwd_IAT_Mean"));
                r.setFwdIATStd(parseDouble(v, indexMap, "Fwd_IAT_Std"));
                r.setFwdIATMax(parseDouble(v, indexMap, "Fwd_IAT_Max"));
                r.setFwdIATMin(parseDouble(v, indexMap, "Fwd_IAT_Min"));

                r.setBwdIATTotal(parseDouble(v, indexMap, "Bwd_IAT_Total"));
                r.setBwdIATMean(parseDouble(v, indexMap, "Bwd_IAT_Mean"));
                r.setBwdIATStd(parseDouble(v, indexMap, "Bwd_IAT_Std"));
                r.setBwdIATMax(parseDouble(v, indexMap, "Bwd_IAT_Max"));
                r.setBwdIATMin(parseDouble(v, indexMap, "Bwd_IAT_Min"));

                r.setFwdPSHFlags(parseInt(v, indexMap, "Fwd_PSH_Flags"));
                r.setBwdPSHFlags(parseInt(v, indexMap, "Bwd_PSH_Flags"));
                r.setFwdURGFlags(parseInt(v, indexMap, "Fwd_URG_Flags"));
                r.setBwdURGFlags(parseInt(v, indexMap, "Bwd_URG_Flags"));

                r.setFwdHeaderLength(parseLong(v, indexMap, "Fwd_Header_Length"));
                r.setBwdHeaderLength(parseLong(v, indexMap, "Bwd_Header_Length"));

                r.setFwdPacketsS(parseDouble(v, indexMap, "Fwd_Packets_s"));
                r.setBwdPacketsS(parseDouble(v, indexMap, "Bwd_Packets_s"));

                r.setMinPacketLength(parseDouble(v, indexMap, "Min_Packet_Length"));
                r.setMaxPacketLength(parseDouble(v, indexMap, "Max_Packet_Length"));
                r.setPacketLengthMean(parseDouble(v, indexMap, "Packet_Length_Mean"));
                r.setPacketLengthStd(parseDouble(v, indexMap, "Packet_Length_Std"));
                r.setPacketLengthVariance(parseDouble(v, indexMap, "Packet_Length_Variance"));

                r.setFINFlagCount(parseInt(v, indexMap, "FIN_Flag_Count"));
                r.setSYNFlagCount(parseInt(v, indexMap, "SYN_Flag_Count"));
                r.setRSTFlagCount(parseInt(v, indexMap, "RST_Flag_Count"));
                r.setPSHFlagCount(parseInt(v, indexMap, "PSH_Flag_Count"));
                r.setACKFlagCount(parseInt(v, indexMap, "ACK_Flag_Count"));
                r.setURGFlagCount(parseInt(v, indexMap, "URG_Flag_Count"));
                r.setCWEFlagCount(parseInt(v, indexMap, "CWE_Flag_Count"));
                r.setECEFlagCount(parseInt(v, indexMap, "ECE_Flag_Count"));

                r.setDownUpRatio(parseDouble(v, indexMap, "Down_Up_Ratio"));

                r.setAveragePacketSize(parseDouble(v, indexMap, "Average_Packet_Size"));
                r.setAvgFwdSegmentSize(parseDouble(v, indexMap, "Avg_Fwd_Segment_Size"));
                r.setAvgBwdSegmentSize(parseDouble(v, indexMap, "Avg_Bwd_Segment_Size"));

                r.setFwdHeaderLength1(parseLong(v, indexMap, "Fwd_Header_Length_1"));

                r.setFwdAvgBytesBulk(parseInt(v, indexMap, "Fwd_Avg_Bytes_Bulk"));
                r.setFwdAvgPacketsBulk(parseInt(v, indexMap, "Fwd_Avg_Packets_Bulk"));
                r.setFwdAvgBulkRate(parseInt(v, indexMap, "Fwd_Avg_Bulk_Rate"));

                r.setBwdAvgBytesBulk(parseInt(v, indexMap, "Bwd_Avg_Bytes_Bulk"));
                r.setBwdAvgPacketsBulk(parseInt(v, indexMap, "Bwd_Avg_Packets_Bulk"));
                r.setBwdAvgBulkRate(parseInt(v, indexMap, "Bwd_Avg_Bulk_Rate"));

                r.setSubflowFwdPackets(parseInt(v, indexMap, "Subflow_Fwd_Packets"));
                r.setSubflowFwdBytes(parseInt(v, indexMap, "Subflow_Fwd_Bytes"));
                r.setSubflowBwdPackets(parseInt(v, indexMap, "Subflow_Bwd_Packets"));
                r.setSubflowBwdBytes(parseInt(v, indexMap, "Subflow_Bwd_Bytes"));

                r.setInitWinBytesForward(parseInt(v, indexMap, "Init_Win_bytes_forward"));
                r.setInitWinBytesBackward(parseInt(v, indexMap, "Init_Win_bytes_backward"));
                r.setActDataPktFwd(parseInt(v, indexMap, "act_data_pkt_fwd"));
                r.setMinSegSizeForward(parseInt(v, indexMap, "min_seg_size_forward"));

                r.setActiveMean(parseDouble(v, indexMap, "Active_Mean"));
                r.setActiveStd(parseDouble(v, indexMap, "Active_Std"));
                r.setActiveMax(parseDouble(v, indexMap, "Active_Max"));
                r.setActiveMin(parseDouble(v, indexMap, "Active_Min"));

                r.setIdleMean(parseDouble(v, indexMap, "Idle_Mean"));
                r.setIdleStd(parseDouble(v, indexMap, "Idle_Std"));
                r.setIdleMax(parseDouble(v, indexMap, "Idle_Max"));
                r.setIdleMin(parseDouble(v, indexMap, "Idle_Min"));

                r.setSimillarHTTP(getString(v, indexMap, "SimillarHTTP"));
                r.setInbound(parseInt(v, indexMap, "Inbound"));
                r.setLabel(getString(v, indexMap, "Label"));

                records.add(r.build());
//                recordCount++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return records;
    }

    private static String getString(String[] v, Map<String, Integer> m, String col) {
        Integer i = m.get(col);
        if (i == null || i >= v.length) return null;
        String val = v[i];
        return val.isEmpty() ? null : val;
    }

    private static Integer parseInt(String[] v, Map<String, Integer> m, String col) {
        String val = getString(v, m, col);
        return val == null ? null : Integer.parseInt(val);
    }

    private static Long parseLong(String[] v, Map<String, Integer> m, String col) {
        String val = getString(v, m, col);
        return val == null ? null : Long.parseLong(val);
    }

    private static Double parseDouble(String[] v, Map<String, Integer> m, String col) {
        String val = getString(v, m, col);
        if (val == null) return null;

        switch (val.toLowerCase()) {
            case "nan": return Double.NaN;
            case "infinity": return Double.POSITIVE_INFINITY;
            case "-infinity": return Double.NEGATIVE_INFINITY;
            default: return Double.parseDouble(val);
        }
    }

    public static Instant parseTimestamp(String[] v, Map<String, Integer> indexMap, String col) {
        try {
            String raw = v[indexMap.get(col)];
            if (raw == null || raw.isEmpty()) return null;

            // Case 1: ISO format
            if (raw.contains("T")) {
                return OffsetDateTime.parse(raw).toInstant();
            }

            // Case 2: space format + microseconds
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime ldt = LocalDateTime.parse(raw, formatter);

            return ldt.atZone(ZoneId.of("UTC")).toInstant();

        } catch (Exception e) {
            System.out.println("Invalid timestamp: " + v[indexMap.get(col)]);
            return null;
        }
    }
}