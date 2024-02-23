import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

public class dataGenerator {

    public static void main(String[] args) throws Exception{

        //Change to 5,000,000 people
        int sizePeople = 50;
        int sizeInfected = 10;
        int maxValueXY = 10000;

        createPeopleCSVs(sizePeople, sizeInfected, maxValueXY);

        //Change to 50,000 customers
        int sizeCustomers = 50000;
        //Change to 5,000,000 purchases
        int sizePurchases = 5000000;

        createTransactionsCSVs(sizeCustomers, sizePurchases);
    }

    public static void createPeopleCSVs(int sizePeople, int sizeInfected, int maxValue) {

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            Path peoplePath = new Path("src/main/data/people/PEOPLE-large.csv");
            FSDataOutputStream outputStream1 = fs.create(peoplePath);
            PrintWriter peopleWriter = new PrintWriter(new OutputStreamWriter(outputStream1));

            Path infectedPath = new Path("src/main/data/people/INFECTED-small.csv");
            FSDataOutputStream outputStream2 = fs.create(infectedPath);
            PrintWriter infectedWriter = new PrintWriter(new OutputStreamWriter(outputStream2));

            Path allPath = new Path("src/main/data/people/PEOPLE-SOME-INFECTED-large.csv");
            FSDataOutputStream outputStream3 = fs.create(allPath);
            PrintWriter allWriter = new PrintWriter(new OutputStreamWriter(outputStream3));

            Random random = new Random();

            //Get random people's ids for infected people
            List<Integer> randomIDs = new ArrayList<>();
            for(int i = 0; i < sizeInfected; i++) {
                randomIDs.add(random.nextInt(sizePeople));
            }

            // Write data
            for (int i = 0; i < sizePeople; i++) {
                int x = random.nextInt(maxValue + 1);
                int y = random.nextInt(maxValue + 1);

                if(randomIDs.contains(i)) {
                    infectedWriter.println(i + "," + x + "," + y);
                    allWriter.println(i + "," + x + "," + y + ",yes");
                } else {
                    allWriter.println(i + "," + x + "," + y + ",no");
                }
                peopleWriter.println(i + "," + x + "," + y);
            }

            peopleWriter.close();
            outputStream1.close();

            infectedWriter.close();
            outputStream2.close();

            allWriter.close();
            outputStream3.close();

        } catch (IOException e) {
            System.out.println("CSV file failed.");
        }
    }

    public static void createTransactionsCSVs(int sizeCustomers, int sizePurchases) {

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            Path customersPath = new Path("src/main/data/transactions/customers.csv");
            FSDataOutputStream outputStream1 = fs.create(customersPath);
            PrintWriter customersWriter = new PrintWriter(new OutputStreamWriter(outputStream1));

            Path purchasesPath = new Path("src/main/data/transactions/purchases.csv");
            FSDataOutputStream outputStream2 = fs.create(purchasesPath);
            PrintWriter purchasesWriter = new PrintWriter(new OutputStreamWriter(outputStream2));

            Random random = new Random();

            // Write data
            for (int i = 0; i < sizeCustomers; i++) {
                //Generate all fields
                int ID = i+1;
                String Name = randomString(10,20);
                int Age = random.nextInt(100-18+1)+18;
                int CountryCode = random.nextInt(500)+1;
                float Salary = (random.nextFloat() * (10000000-100)) + 100;

                customersWriter.println(ID + "," + Name + "," + Age + "," + CountryCode + "," + Salary);
            }

            for (int i = 0; i < sizePurchases; i++) {
                //Generate all fields
                int TransID = i+1;
                int CustID = random.nextInt(sizeCustomers)+1;
                float TransTotal = (random.nextFloat() * (2000-10)) + 10;
                int TransNumItems = random.nextInt(15)+1;
                String TransDesc = randomString(20,50);

                purchasesWriter.println(TransID + "," + CustID + "," + TransTotal + "," + TransNumItems + "," + TransDesc);
            }

            customersWriter.close();
            outputStream1.close();

            purchasesWriter.close();
            outputStream2.close();

        } catch (IOException e) {
            System.out.println("CSV file failed.");
        }
    }

    private static String randomString(int min, int max) {
        RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
        int length = (new Random()).nextInt(max-min+1)+min;
        return generator.generate(length);
    }

    public static List<String> getListFromFile(String fileName) {
        List<String> output = new ArrayList<>();
        File file = new File(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                output.add(line);
            }
            IOUtils.closeStream(reader);
        } catch(FileNotFoundException e) {
            System.out.println("File not found");
        } catch(IOException e) {
            System.out.println(e);
        }
        return output;
    }

}
