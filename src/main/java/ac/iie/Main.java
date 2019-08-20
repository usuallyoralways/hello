package ac.iie;

import io.hops.exception.StorageException;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;

import static ac.iie.nnts.file.ToTimescaleDB.importData;

public class Main {

    public static void main(String[] args) throws SQLException, StorageException, FileNotFoundException, UnsupportedEncodingException {

        URLDecoder decoder = new URLDecoder();


        String filename = "D:\\Documents\\vscode\\data\\shuttle.csv";
        String filepath = decoder.decode(filename,"utf-8");

//        importData("D:/Documents/?????/CODE/pljava-code/src/main/resources/tao.csv");
        importData(filepath);

    }
}
