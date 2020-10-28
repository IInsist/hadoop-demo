package com.hadoop.demo.mr.studentInfo;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author fujh
 * @since 2020年10月27日17:11:50
 * 操作文件基本字段对应实体类
 */
@Data
@AllArgsConstructor
public class StudentInfo implements Writable {

    private String xh;
    private String xm;
    private String sfz;
    private String lxdh;
    private String nj;
    private String bj;
    private String bzr;
    private String xy;
    private String xx;
    private String cjsj;

    public StudentInfo() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.xh);
        out.writeUTF(this.xm);
        out.writeUTF(this.sfz);
        out.writeUTF(this.lxdh);
        out.writeUTF(this.nj);
        out.writeUTF(this.bj);
        out.writeUTF(this.bzr);
        out.writeUTF(this.xy);
        out.writeUTF(this.xx);
        out.writeUTF(this.cjsj);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.xh = in.readUTF();
        this.xm = in.readUTF();
        this.sfz = in.readUTF();
        this.lxdh = in.readUTF();
        this.nj = in.readUTF();
        this.bj = in.readUTF();
        this.bzr = in.readUTF();
        this.xy = in.readUTF();
        this.xx = in.readUTF();
        this.cjsj = in.readUTF();
    }
}
