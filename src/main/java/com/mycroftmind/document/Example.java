/**
 * Copyright (C) ${year} Marvin Herman Froeder (marvin@marvinformatics.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mycroftmind.document;

import org.elasticsearch.search.SearchHit;

import java.util.Objects;

public class Example {

    private final String id;
    private final String speaker;
    private final String line_number;
    private final String play_name;

    public Example(String id,
            String speaker,
            String line_number,
            String play_name) {
        this.id = id;
        this.speaker = speaker;
        this.line_number = line_number;
        this.play_name = play_name;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Example sourceToExample(final SearchHit source) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(source.getSource());

        return builder()
                .id(source.getId())
                .speaker((String) source.getSource().get("speaker"))
                .line_number((String) source.getSource().get("line_number"))
                .play_name((String) source.getSource().get("play_name"))
                .build();
    }

    @Override
    public String toString() {
        return "Example{" +
                "id='" + id + '\'' +
                ", speaker='" + speaker + '\'' +
                ", line_number='" + line_number + '\'' +
                ", play_name='" + play_name + '\'' +
                '}';
    }

    public static class Builder {

        private String id;
        private String speaker;
        private String line_number;
        private String play_name;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder speaker(String speaker) {
            this.speaker = speaker;
            return this;
        }

        public Builder line_number(String line_number) {
            this.line_number = line_number;
            return this;
        }

        public Builder play_name(String play_name) {
            this.play_name = play_name;
            return this;
        }

        public Example build() {
            Objects.requireNonNull(this.id);
            Objects.requireNonNull(this.speaker);
            Objects.requireNonNull(this.line_number);
            Objects.requireNonNull(this.play_name);

            return new Example(this.id,
                    this.speaker,
                    this.line_number,
                    this.play_name);
        }

    }

}
