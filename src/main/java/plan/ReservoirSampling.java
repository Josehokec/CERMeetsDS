package plan;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;

public class ReservoirSampling{
    public int maxSampleNum = 5000;
    public List<String> samples = null;
    public Random random = new Random(7);

    public ReservoirSampling(int maxSampleNum){
        this.maxSampleNum = maxSampleNum;
        this.samples = new ArrayList<>(maxSampleNum);
    }

    public final void sampling(String line, int recordIndices){
        Random random = new Random();
        if(recordIndices < maxSampleNum){
            samples.add(line);
        }
        else{
            // [0, num)
            int r = random.nextInt(recordIndices + 1);
            if (r < maxSampleNum) {
                // replace
                samples.set(r, line);
            }
        }
    }

    public void getSamples(String filePath, String outputPath){
        // read file line by line
        // for each line, call sampling(line, recordIndices)
        // recordIndices starts from 0 and increases by 1 for each line
        File file = new File(filePath);

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int recordIndices = 0;
            br.readLine();
            while ((line = br.readLine()) != null) {
                sampling(line, recordIndices);
                recordIndices++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Files.write(Paths.get(outputPath), samples);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        String filePath = "src/main/dataset/synthetic50M.csv";
        String outputPath =  "src/main/dataset/synthetic50M_sampling.csv";
        ReservoirSampling reservoirSampling = new ReservoirSampling(5000);
        reservoirSampling.getSamples(filePath, outputPath);
    }
}