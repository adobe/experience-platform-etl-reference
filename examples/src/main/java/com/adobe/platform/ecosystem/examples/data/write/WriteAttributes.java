
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

/**
 * @author vardgupt
 *
 */
public class WriteAttributes {

    private Boolean isFlushStrategyRequired;

    private long sizeOfRecord;

    private FlushHandler flushHandler;

    private boolean isEOF;

    private WriteAttributes(WriteAttributesBuilder writeAttributesBuilder) {
        this.setFlushStrategyRequired(writeAttributesBuilder.getIsFlushStrategyRequired());
        this.setSizeOfRecord(writeAttributesBuilder.getSizeOfRecord());
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

    public static class WriteAttributesBuilder{

        private Boolean isFlushStrategyRequired = false;

        private long sizeOfRecord;

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

        public WriteAttributes build(){
            return new WriteAttributes(this);
        }
    }
}