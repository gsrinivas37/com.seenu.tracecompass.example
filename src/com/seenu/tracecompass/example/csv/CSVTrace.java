package com.seenu.tracecompass.example.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.event.ITmfEventField;
import org.eclipse.tracecompass.tmf.core.event.TmfEvent;
import org.eclipse.tracecompass.tmf.core.event.TmfEventField;
import org.eclipse.tracecompass.tmf.core.event.TmfEventType;
import org.eclipse.tracecompass.tmf.core.exceptions.TmfTraceException;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfContext;
import org.eclipse.tracecompass.tmf.core.trace.ITmfEventParser;
import org.eclipse.tracecompass.tmf.core.trace.TmfContext;
import org.eclipse.tracecompass.tmf.core.trace.TmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TraceValidationStatus;
import org.eclipse.tracecompass.tmf.core.trace.location.ITmfLocation;
import org.eclipse.tracecompass.tmf.core.trace.location.TmfLongLocation;

import com.seenu.tracecompass.example.Activator;

public class CSVTrace extends TmfTrace implements ITmfEventParser{

	ITmfLocation currentLoc = null;
	TmfLongLocation fCurrent;

	long count;

	private long initOffset;
	private long currentChunk;

	private File fFile;
	private String[] fEventTypes;
	private FileChannel fFileChannel;
	private MappedByteBuffer fMappedByteBuffer;

	private static final long CHUNK_SIZE = 65536;

	public CSVTrace() {
	}

	public CSVTrace(IResource resource, Class<? extends ITmfEvent> type, String path, int cacheSize, long interval) throws TmfTraceException {
		super(resource, type, path, cacheSize, interval);
	}

	public CSVTrace(TmfTrace trace) throws TmfTraceException {
		super(trace);
	}

	@Override
	public IStatus validate(IProject project, String path) {
		File f = new File(path);
		if (!f.exists()) {
			return new Status(IStatus.ERROR, Activator.PLUGIN_ID,
					"File does not exist"); //$NON-NLS-1$
		}
		if (!f.isFile()) {
			return new Status(IStatus.ERROR, Activator.PLUGIN_ID, path
					+ " is not a file"); //$NON-NLS-1$
		}
		return new TraceValidationStatus(21, Activator.CSV_TRACE_TYPE_ID); //$NON-NLS-1$
	}

	private String[] readHeader(File file) {
		System.out.println("");
		String header = new String();
		try (BufferedReader br = new BufferedReader(new FileReader(file));) {
			header = br.readLine();
		} catch (IOException e) {
		}
		initOffset = header.length() + 1;
		header = header.trim();
		if(header.endsWith(",")){
			header = header.substring(0, header.length()-1);
		}

		return header.split(","); //$NON-NLS-1$
	}

	@Override
	public void initTrace(IResource resource, String path, Class<? extends ITmfEvent> type, String name, String traceTypeId) throws TmfTraceException {
		super.initTrace(resource, path, type, name, traceTypeId);
		fFile = new File(path);
		fFile.length();
		fEventTypes = readHeader(fFile);
		count = 0 ;
		try {
			fFileChannel = new FileInputStream(fFile).getChannel();
			currentChunk = 0;
			seekChunk(currentChunk);
		} catch (IOException e) {
		}
	}

	private void seekChunk(long chunkNum) throws IOException {
		final long position = initOffset + chunkNum*CHUNK_SIZE;
		long size = Math.min((fFileChannel.size()-position), CHUNK_SIZE);
		if(size<0){
			System.out.println("ERROR: $$$$$");
		}

		fMappedByteBuffer = fFileChannel.map(MapMode.READ_ONLY, position, size);
		currentLoc = new TmfLongLocation(position);
	}

	@Override
	public ITmfLocation getCurrentLocation() {
		return currentLoc;
	}

	@Override
	public double getLocationRatio(ITmfLocation location) {
		return ((TmfLongLocation) location).getLocationInfo().doubleValue()/getNbEvents();
	}

	@Override
	public ITmfContext seekEvent(ITmfLocation location) {
		TmfLongLocation tl = null;

		if(location==null){
			tl = new TmfLongLocation(0);
		}else{
			tl = (TmfLongLocation) location;
		}

		Long longVal = tl.getLocationInfo();
		long chunkVal = longVal/CHUNK_SIZE;
		long remainder = longVal % CHUNK_SIZE;

		if(chunkVal!=currentChunk){
			try {
				seekChunk(chunkVal);
				currentChunk = chunkVal;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if(remainder<fMappedByteBuffer.limit()){
			fMappedByteBuffer.position((int) remainder);
		}

		return new TmfContext(tl);
	}

	@Override
	public ITmfContext seekEvent(double ratio) {
		return seekEvent((long)ratio*getNbEvents()/100);
	}

	@Override
	public ITmfEvent parseEvent(ITmfContext context) {
		TmfLongLocation location = (TmfLongLocation) context.getLocation();
		Long info = location.getLocationInfo();
		TmfEvent event = null;
		StringBuffer buffer;

		System.out.print("Count: "+ ++count);
		System.out.println("; Parsing event rank: "+ context.getRank());

		buffer= new StringBuffer();
		String str = null;
		final TmfEventField[] events = new TmfEventField[fEventTypes.length];
		byte b[] = new byte[1];
		for(int i=0; i< events.length; i++){
			buffer = new StringBuffer();
			if(fMappedByteBuffer.position()==fMappedByteBuffer.limit()){
				if(fMappedByteBuffer.limit()==CHUNK_SIZE){
					try {
						seekChunk(++currentChunk);
						fMappedByteBuffer.get(b);
						str = new String(b);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{
					return null;
				}
			}else{
				fMappedByteBuffer.get(b);
				str = new String(b);
			}
			while(!str.equals(",")){
				if((str.equals("\n")&&i==events.length-1)||(str.equals("\r")&&i==events.length-1)){
					break;
				}

				buffer.append(str);

				if(fMappedByteBuffer.position()==fMappedByteBuffer.limit()){
					if(fMappedByteBuffer.limit()==CHUNK_SIZE){
						try {
							seekChunk(++currentChunk);
							fMappedByteBuffer.get(b);
							str = new String(b);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else{
						return null;
					}
				}else{
					fMappedByteBuffer.get(b);
					str = new String(b);
				}
			};
			events[i] = new TmfEventField(fEventTypes[i], buffer.toString().trim(), null);
		}
		long ts = Long.parseLong(events[0].getValue().toString());

		final TmfEventField content = new TmfEventField(ITmfEventField.ROOT_FIELD_ID, null, events);
		event = new TmfEvent(this, info, new TmfTimestamp(ts, ITmfTimestamp.NANOSECOND_SCALE), new TmfEventType(getTraceTypeId(), content), content);
		currentLoc = new TmfLongLocation(currentChunk*CHUNK_SIZE + fMappedByteBuffer.position());

		return event;
	}
}
