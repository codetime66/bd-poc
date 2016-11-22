import java.io.*;

class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: java Main <outputfile>");
            System.exit(-1);
        }

        try {
            FileWriter out = new FileWriter(args[0]);
            for(int i=1; i<101; i++){
               out.write("FORWARD:0001");
               out.write( (String.valueOf(1000+i)).substring(1) ); 
               out.write("05000005001200011453300005748574857483758475003490950490453490592093409204903259049504950395029402");
               out.write("94205905943069059630459059402394023943205940594305943069069096059405904924023940239409509\n");
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
            
