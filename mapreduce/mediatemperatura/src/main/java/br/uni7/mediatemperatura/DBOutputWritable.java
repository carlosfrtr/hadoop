package br.uni7.mediatemperatura;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable
{
   private String tempo;
   private Double media = 0.0;

   public DBOutputWritable(String tempo, Double media)
   {
     this.tempo = tempo;
     this.media = media;
   }

   public void readFields(DataInput in) throws IOException {   }

   public void readFields(ResultSet rs) throws SQLException
   {
     tempo = rs.getString(1);
     media = rs.getDouble(2);
   }

   public void write(DataOutput out) throws IOException {    }

   public void write(PreparedStatement ps) throws SQLException
   {
     ps.setString(1, tempo);
     ps.setDouble(2, media);
   }
}