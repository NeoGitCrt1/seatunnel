/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.s3enc;

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

@Getter
@Setter
public class S3EncTransformConfig implements Serializable {
    public static final Option<String> endpoint =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "s3 endpoint");
    public static final Option<String> ak =
            Options.key("ak")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "s3 ak");
    public static final Option<String> sk =
            Options.key("sk")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "s3 sk");
    public static final Option<String> bucket =
            Options.key("bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "s3 bucket");
    public static final Option<String> resize =
            Options.key("resize")
                    .stringType()
                    .defaultValue("512")
                    .withDescription(
                            "s3 img l_resize to");
    public static final Option<String> split_by =
            Options.key("split_by")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "s3 key will split to multi keys");
}
