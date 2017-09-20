package cz.scholz.kafka.kafkaclients.util;

import java.util.Random;

public class RandomStringGenerator {
    private static String SALTCHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    public static String getSaltString(int length) {
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

        /*return "Lorem ipsum dolor sit amet, consectetur adipiscing elit, \n" +
                "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. \n" +
                "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris \r" +
                "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in \r" +
                "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \r\n" +
                "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \r\n" +
                "culpa qui officia deserunt mollit anim id est laborum.";*/
    }
}
