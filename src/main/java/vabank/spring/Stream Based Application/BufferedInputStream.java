package vabank.spring;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;


public class BufferedInputStream extends BufferedInputStream
{
    public BufferedInputStream(InputStream in)
    {
        super(in,1024);
    }

    public void resetBuffer()
    {
        this.count=this.pos=this.marklimit=0;
        this.markpos=-1;
    }

    public void setInputStream(InputStream in)
    {
        resetBuffer();
        this.in=in;
    }

    @Override
    public void close() throws IOException {
        InputStream input = in;
        in = null;
        if (input != null)
            input.close();
        return;
    }
}