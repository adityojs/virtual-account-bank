import java.io.BufferedOutputStream;
import java.io.OutputStream;

public class BufferedOutputStream extends BufferedOutputStream
{
    public BufferedOutputStream(OutputStream out)
    {
        super(out,1024);
    }

    public void resetBuffer()
    {
        count=0;
    }

    public void setOutputStream(OutputStream out)
    {
        resetBuffer();
        this.out=out;
    }
}
