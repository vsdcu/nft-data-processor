package org.dcu.util;

import java.util.Random;

public class RandomNameGenerator {
    private static final String[] ADJECTIVES = {"Bold", "Swift", "Loud", "Sharp", "Brave", "Vivid", "Cool", "Sly", "Neat", "Huge", "Keen", "Deep", "Calm", "Lush", "Wild", "Wise", "Nice", "Tall", "Fast", "Bright"};
    private static final String[] NOUNS = {"Lion", "Tiger", "Bear", "Wolf", "Hawk", "Fox", "Snake", "Raven", "Eagle", "Shark", "Dolphin", "Jaguar", "Panther", "Cougar", "Leopard", "Gorilla", "Rhino", "Saber", "Sword", "Arrow"};

    private static final String[] FIRSTNAMES = {"Emily",  "Jacob",  "Avery",  "Michael",  "Sofia",  "Cameron",  "Isabella",  "Ethan",  "Liam",  "Nora",  "Olivia",  "James",  "Lucas",  "Mia",  "Hannah",  "Madison",  "Ella",  "William",  "Benjamin",  "Ava"};

    private static final String[] LASTNAMES = {"Lee",  "Ng",  "Wong",  "Kim",  "Chan",  "Goh",  "Tan",  "Loh",  "Yeo",  "Koh",  "Teo",  "Ong",  "Lim",  "Tay",  "Chua",  "Toh",  "Pang",  "Kwan",  "Cheong",  "Lau"};

    public static String generateRandomMintersName() {
        Random rand = new Random();
        String adjective = ADJECTIVES[rand.nextInt(ADJECTIVES.length)];
        String noun = NOUNS[rand.nextInt(NOUNS.length)];
        int randomNumber = rand.nextInt(1000); // generate a random number between 0 and 999
        return adjective + " " + noun + " " + randomNumber;
    }

    public static String generateRandomBuyersSellersName() {
        Random rand = new Random();
        String adjective = FIRSTNAMES[rand.nextInt(FIRSTNAMES.length)];
        String noun = LASTNAMES[rand.nextInt(LASTNAMES.length)];
        int randomNumber = rand.nextInt(1000); // generate a random number between 0 and 999
        return adjective + " " + noun + " " + randomNumber;
    }

}