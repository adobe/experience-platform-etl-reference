
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

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ParquetIOUtil {

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