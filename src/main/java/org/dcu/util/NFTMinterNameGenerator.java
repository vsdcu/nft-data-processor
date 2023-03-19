package org.dcu.util;

import java.util.Random;

public class NFTMinterNameGenerator {
    private static final String[] ADJECTIVES = {"Bold", "Swift", "Loud", "Sharp", "Brave", "Vivid", "Cool", "Sly", "Neat", "Huge", "Keen", "Deep", "Calm", "Lush", "Wild", "Wise", "Nice", "Tall", "Fast", "Bright"};
    private static final String[] NOUNS = {"Lion", "Tiger", "Bear", "Wolf", "Hawk", "Fox", "Snake", "Raven", "Eagle", "Shark", "Dolphin", "Jaguar", "Panther", "Cougar", "Leopard", "Gorilla", "Rhino", "Saber", "Sword", "Arrow"};

    public static String generateRandomName() {
        Random rand = new Random();
        String adjective = ADJECTIVES[rand.nextInt(ADJECTIVES.length)];
        String noun = NOUNS[rand.nextInt(NOUNS.length)];
        int randomNumber = rand.nextInt(1000); // generate a random number between 0 and 999
        return adjective + " " + noun + " " + randomNumber;
    }

}