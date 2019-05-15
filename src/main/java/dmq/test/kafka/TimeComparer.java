package dmq.test.kafka;

// 打印两个时间值之间的差值（使用System.currentTimeMillis）
public final class TimeComparer {
    private final String tcName;
    private long lastTime, lastIndex;
    private long topDelta1, topDelta2, topDelta3, topDelta4, topDelta5;
    private long idxDelta1, idxDelta2, idxDelta3, idxDelta4, idxDelta5;
    public TimeComparer(String name) {
        topDelta1= topDelta2= topDelta3= topDelta4= topDelta5= 0;
        tcName= name;
        lastIndex= 1;
        System.out.printf("(TC %d: %s) ready\n", lastIndex, tcName);
        lastTime= System.currentTimeMillis();
    }

    public void reset() {
        reset(null);
    }
    public void reset(String desc) {
        if(desc != null)
            System.out.printf("(TC %d: %s) reset for %s\n", lastIndex, tcName, desc);
        else
            System.out.printf("(TC %d: %s) reset\n", lastIndex, tcName);
        lastTime= System.currentTimeMillis();
    }

    public void update() {
        long time= System.currentTimeMillis();
        long delta= time - lastTime;
        long index= lastIndex++;
        lastTime= time;
        //
        System.out.printf("(TC %d: %s) +++++ RECENT= %d +++++ TOP5= [%d-%d, %d-%d, %d-%d, %d-%d, %d-%d]\n", index, tcName, delta,
                idxDelta1, topDelta1, idxDelta2, topDelta2, idxDelta3, topDelta3, idxDelta4, topDelta4, idxDelta5, topDelta5);
        // 更新topDeltaN
        updateTopDeltas(index, delta);
    }
    private void updateTopDeltas(long index, long delta) {
        if(delta > topDelta1) {
            updateTopDeltas(idxDelta1, topDelta1);
            idxDelta1= index;
            topDelta1= delta;
        } else if(delta > topDelta2) {
            updateTopDeltas(idxDelta2, topDelta2);
            idxDelta2= index;
            topDelta2= delta;
        } else if(delta > topDelta3) {
            updateTopDeltas(idxDelta3, topDelta3);
            idxDelta3= index;
            topDelta3= delta;
        } else if(delta > topDelta4) {
            updateTopDeltas(idxDelta4, topDelta4);
            idxDelta4= index;
            topDelta4= delta;
        } else if(delta > topDelta5) {
            updateTopDeltas(idxDelta5, topDelta5);
            idxDelta5= index;
            topDelta5= delta;
        }
    }
}
