/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2014-2017 Marc de Verdelhan, 2017-2019 Ta4j Organization & respective
 * authors (see AUTHORS)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ta4jexamples.loaders;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeries;

/**
 * This class build a Ta4j bar series from a JSON file containing bars.
 */
public class JsonBarsLoader {

    public static BarSeries load(String filename) {
        BarSeries series = new BaseBarSeries("btcusdt_bars");

        String res = "";
        try (BufferedInputStream is = new BufferedInputStream(
                JsonBarsLoader.class.getClassLoader().getResourceAsStream(filename));
                ByteArrayOutputStream os = new ByteArrayOutputStream(200 * 1024); // 200k
        ) {
            int c = -1;
            byte[] b = new byte[8192];
            while ((c = is.read(b)) != -1) {
                os.write(b, 0, c);
            }

            res = os.toString("UTF-8");
        } catch (IOException ioe) {
            Logger.getLogger(CsvBarsLoader.class.getName()).log(Level.SEVERE, "Unable to load bars from JSON", ioe);
        } catch (NumberFormatException nfe) {
            Logger.getLogger(CsvBarsLoader.class.getName()).log(Level.SEVERE, "Error while parsing value", nfe);
        }

        if (StringUtils.isEmpty(res)) {
            Logger.getLogger(CsvBarsLoader.class.getName()).log(Level.SEVERE, "Empty json file: {}", filename);
            return series;
        }

        JSONArray arr = JSONObject.parseArray(res);
        for (int i = 0; i < arr.size(); ++i) {
            JSONArray a = arr.getJSONArray(i);

            ZonedDateTime date = ZonedDateTime.ofInstant(new Date(a.getLongValue(6)).toInstant(), ZoneOffset.UTC);
            BigDecimal open = a.getBigDecimal(1);
            BigDecimal high = a.getBigDecimal(2);
            BigDecimal low = a.getBigDecimal(3);
            BigDecimal close = a.getBigDecimal(4);
            BigDecimal volume = a.getBigDecimal(5);
            BigDecimal amount = a.getBigDecimal(7);
            int trades = a.getIntValue(8);

            series.addBar(Duration.ofDays(1), date, open, high, low, close, volume, amount);
        }

        return series;
    }

}
