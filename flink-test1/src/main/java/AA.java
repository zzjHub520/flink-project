import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class AA {
    public static void main(String[] args) throws IOException {
        String fileName = "input/test.txt";
        String line = "";

        BufferedReader in = new BufferedReader(new FileReader(fileName));
        line = in.readLine();
        String pattern = "^P.*";
        while (line != null) {
            boolean matches = Pattern.matches(pattern, line);
            if (matches) {
                String content = in.readLine();
                String time = in.readLine();
                String substring = line.substring(1);
                System.out.println("P" + String.format( "%03d",Integer.parseInt(substring)) + " - " + time + " - " + content);
            }
            line = in.readLine();
        }
        in.close();

    }
}
