
/*
 *  Copyright 2017-2018 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.platform.ecosystem.examples.parquet.utility;

import org.apache.parquet.io.api.Binary;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ParquetIOUtil {

    private final static long JULIAN_DAY_OF_EPOCH = 2440588;

    public static String generateRandomText() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

    public static long dateFromInt96( Binary value ) {
        byte[] readBuffer = value.getBytes();
        if ( readBuffer.length != 12 ) {
            throw new RuntimeException( "Invalid byte array length for INT96" );
        }

        long timeOfDayNanos =
                ( ( (long) readBuffer[7] << 56 ) + ( (long) ( readBuffer[6] & 255 ) << 48 )
                        + ( (long) ( readBuffer[5] & 255 ) << 40 ) + ( (long) ( readBuffer[4] & 255 ) << 32 )
                        + ( (long) ( readBuffer[3] & 255 ) << 24 ) + ( ( readBuffer[2] & 255 ) << 16 )
                        + ( ( readBuffer[1] & 255 ) << 8 ) + ( ( readBuffer[0] & 255 ) << 0 ) );

        int julianDay =
                ( (int) ( readBuffer[11] & 255 ) << 24 ) + ( ( readBuffer[10] & 255 ) << 16 )
                        + ( ( readBuffer[9] & 255 ) << 8 ) + ( ( readBuffer[8] & 255 ) << 0 );

        return ( julianDay - JULIAN_DAY_OF_EPOCH ) * 24L * 60L * 60L * 1000L + timeOfDayNanos / 1000000;
    }

    public static File getLocalFilePath(String fileName) {
        String suffix = fileName.endsWith(".parquet") ? "" : ".parquet";
        File tempFile = null;
        try {
            tempFile = File.createTempFile(fileName, suffix);
            if (!tempFile.delete()) {
                throw new RuntimeException("Unable to delete the local temp file for logs");
            }
        } catch (IOException e) {
            throw new RuntimeException("Caught IO exception for creating temp file. " + e);
        }
        return tempFile;
    }
}