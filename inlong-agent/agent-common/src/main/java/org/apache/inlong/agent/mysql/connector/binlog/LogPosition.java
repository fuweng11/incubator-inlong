package org.apache.inlong.agent.mysql.connector.binlog;

import java.util.Objects;

/**
 * Implements binlog position.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class LogPosition implements Cloneable, Comparable<LogPosition>
{
    /* binlog file's name */
    protected String fileName;

    /* position in file */
    protected long   position;

    /**
     * Binlog position init.
     * 
     * @param fileName file name for binlog files: mysql-bin.000001
     */
    public LogPosition(String fileName)
    {
        this.fileName = fileName;
        this.position = 0L;
    }

    /**
     * Binlog position init.
     * 
     * @param fileName file name for binlog files: mysql-bin.000001
     */
    public LogPosition(String fileName, final long position)
    {
        this.fileName = fileName;
        this.position = position;
    }

    /**
     * Binlog position copy init.
     */
    public LogPosition(LogPosition source)
    {
        this.fileName = source.fileName;
        this.position = source.position;
    }

    public final String getFileName()
    {
        return fileName;
    }

    public final long getPosition()
    {
        return position;
    }

    /* Clone binlog position without CloneNotSupportedException */
    public LogPosition clone()
    {
        try
        {
            return (LogPosition) super.clone();
        }
        catch (CloneNotSupportedException e)
        {
            // Never happend
            return null;
        }
    }

    /**
     * Compares with the specified fileName and position.
     */
    public final int compareTo(String fileName, final long position)
    {
        final int val = this.fileName.compareTo(fileName);

        if (val == 0)
        {
            return (int) (this.position - position);
        }
        return val;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(LogPosition o)
    {
        final int val = fileName.compareTo(o.fileName);

        if (val == 0)
        {
            return (int) (position - o.position);
        }
        return val;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogPosition that = (LogPosition) o;
        return position == that.position && fileName.equals(that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, position);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return fileName + ':' + position;
    }
}
