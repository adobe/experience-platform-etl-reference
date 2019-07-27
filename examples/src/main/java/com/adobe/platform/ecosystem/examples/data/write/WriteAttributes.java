
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
package com.adobe.platform.ecosystem.examples.data.write;

import java.util.Map;
import java.util.Set;

/**
 * @author vardgupt
 *
 */
public class WriteAttributes {

    private Boolean isFlushStrategyRequired;

    private long sizeOfRecord;

    private FlushHandler flushHandler;

    private boolean isEOF;

    private boolean fullSchemaRequired;

    private boolean validateData;

    private Map<String,Set<String>> batchTags;

    private WriteAttributes(WriteAttributesBuilder writeAttributesBuilder) {
        this.setFlushStrategyRequired(writeAttributesBuilder.getIsFlushStrategyRequired());
        this.setSizeOfRecord(writeAttributesBuilder.getSizeOfRecord());
        this.setFullSchemaRequired(writeAttributesBuilder.isFullSchemaRequired());
        this.setValidateData(writeAttributesBuilder.getValidateData());
        if(sizeOfRecord>0 && isFlushStrategyRequired){
            setFlushHandler(new FlushHandler(sizeOfRecord));
        }
    }
    public Boolean isFlushStrategyRequired() {
        return isFlushStrategyRequired;
    }
    public void setFlushStrategyRequired(Boolean isFlushStrategyRequired) {
        this.isFlushStrategyRequired = isFlushStrategyRequired;
    }

    public void setSizeOfRecord(long sizeOfRecord) {
        this.sizeOfRecord = sizeOfRecord;
    }

    public FlushHandler getFlushHandler() {
        return flushHandler;
    }
    public void setFlushHandler(FlushHandler flushHandler) {
        this.flushHandler = flushHandler;
    }

    public boolean isEOF() {
        return isEOF;
    }
    public void setEOF(boolean isEOF) {
        this.isEOF = isEOF;
    }

    private void setBatchTags(Map<String,Set<String>> batchTags)
    {
        this.batchTags = batchTags;
    }
    public Map<String,Set<String>> getBatchTags()
    {
        if(batchTags == null || batchTags.isEmpty())
            return null;
        return batchTags;
    }

    /**
     * Invoked when full schema is required by the user,
     * in the case of varying fields in different data records.
     *
     * If the set of fields remain consistent across data records in a single API call,
     * this flag can be set to false.
     *
     */
    public boolean isFullSchemaRequired() {
        return fullSchemaRequired;
    }

    public WriteAttributes setFullSchemaRequired(boolean fullSchemaRequired) {
        this.fullSchemaRequired = fullSchemaRequired;
        return this;
    }

    /**
     * Invoked on user demand of data validation via schema fields.
     *
     */
    public void setValidateData(boolean validateData) {
        this.validateData = validateData;
    }

    public boolean isDataValidationWithSchema() {
        return validateData;
    }

    public static class WriteAttributesBuilder{

        private Boolean isFlushStrategyRequired = false;

        private long sizeOfRecord;

        private Map<String,Set<String>> batchTags;

        //Setting default value to false.
		private boolean isFullSchemaRequired = false;

        //Setting default to false.
        private boolean validateData = false;

        public Boolean getIsFlushStrategyRequired() {
            return isFlushStrategyRequired;
        }

        public WriteAttributesBuilder withFlushStrategy(Boolean isFlushStrategyRequired) {
            this.isFlushStrategyRequired = isFlushStrategyRequired;
            return this;
        }

        public long getSizeOfRecord() {
            return sizeOfRecord;
        }

        public WriteAttributesBuilder withSizeOfRecord(long sizeOfRecord) {
            this.sizeOfRecord = sizeOfRecord;
            return this;
        }

        public Map<String, Set<String>> getBatchTags() {
            return batchTags;
        }

        public WriteAttributesBuilder withBatchTags(Map<String, Set<String>> batchTags) {
            this.batchTags = batchTags;
            return this;
        }

        public boolean isFullSchemaRequired() {
            return isFullSchemaRequired;
        }

        public boolean getValidateData() {
            return validateData;
        }

        public WriteAttributes build(){
            return new WriteAttributes(this);
        }
    }
}