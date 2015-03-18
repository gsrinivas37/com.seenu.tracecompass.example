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

	private int fOffset;
	private File fFile;
	private String[] fEventTypes;
	private FileChannel fFileChannel;
	private MappedByteBuffer fMappedByteBuffer;

	private static final int CHUNK_SIZE = 65536;

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
		String header = new String();
		try (BufferedReader br = new BufferedReader(new FileReader(file));) {
			header = br.readLine();
		} catch (IOException e) {
		}
		fOffset = header.length() + 1;
		return header.split(","); //$NON-NLS-1$
	}

	@Override
	public void initTrace(IResource resource, String path, Class<? extends ITmfEvent> type, String name, String traceTypeId) throws TmfTraceException {
		super.initTrace(resource, path, type, name, traceTypeId);
		fFile = new File(path);
		fFile.length();
		fEventTypes = readHeader(fFile);

		try {
			fFileChannel = new FileInputStream(fFile).getChannel();
			seek(0);
		} catch (IOException e) {
		}
	}

	private void seek(long rank) throws IOException {
		final int position = fOffset;
		int size = Math.min((int) (fFileChannel.size() - position), CHUNK_SIZE);
		fMappedByteBuffer = fFileChannel.map(MapMode.READ_ONLY, position, size);

		currentLoc = new TmfLongLocation(fOffset);
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


		if(tl.getLocationInfo()<fMappedByteBuffer.limit()){
			fMappedByteBuffer.position(tl.getLocationInfo().intValue());
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

		if(fMappedByteBuffer.position()+fEventTypes.length>fMappedByteBuffer.limit()){
			return null;
		}

		buffer= new StringBuffer();
		String str;
		final TmfEventField[] events = new TmfEventField[fEventTypes.length];
		byte b[] = new byte[1];
		for(int i=0; i< events.length; i++){
			buffer = new StringBuffer();
			fMappedByteBuffer.get(b);
			str = new String(b);
			while(!str.equals(",")){
				if((str.equals("\n")&&i==events.length-1)||(str.equals("\r")&&i==events.length-1)){
					break;
				}else{
					System.out.println("");
				}

				buffer.append(str);

				if(fMappedByteBuffer.position()==fMappedByteBuffer.limit()){
					str = "\n";
				}else{
					fMappedByteBuffer.get(b);
					str = new String(b);
				}
			};
			events[i] = new TmfEventField(fEventTypes[i], buffer.toString(), null);
		}
		long ts = Long.parseLong(events[0].getValue().toString());

		final TmfEventField content = new TmfEventField(ITmfEventField.ROOT_FIELD_ID, null, events);
		event = new TmfEvent(this, info, new TmfTimestamp(ts, ITmfTimestamp.NANOSECOND_SCALE), new TmfEventType(getTraceTypeId(), content), content);
		currentLoc = new TmfLongLocation(fMappedByteBuffer.position());

		return event;
	}
}
