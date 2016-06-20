package bg.government.opendatapusher;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

public class PusherTest {

    @Test
    public void parserTest() throws Exception {
        Path path = Paths.get("./src/test/resources/pusher.yml");
        List<PushConfig> configs = Pusher.parseConfig(path.toAbsolutePath().toString()).getConfigs();
        assertThat(configs.size(), equalTo(1));
        assertThat(configs.get(0).getSourceType(), equalTo(SourceType.XLS));
        assertThat(configs.get(0).getTitle(), equalTo("test-xls"));
    }
    
    @Test
    public void xlsToCsvTest() throws IOException {
        Path path = Paths.get("./src/test/resources/test.xls");
        String resultPath = convert(path);
        System.out.println(resultPath);
    }
    
    @Test
    public void xlsxToCsvTest() throws IOException {
        Path path = Paths.get("./src/test/resources/test.xlsx");
        String resultPath = convert(path);
        System.out.println(resultPath);
    }

    private String convert(Path path) throws IOException {
        PushConfig conf = new PushConfig();
        conf.setPath(path.toAbsolutePath().toString());
        Pusher pusher = new Pusher(conf, "");
        String resultPath = pusher.xlsToCsv(conf.getPath());
        return resultPath;
    }
    
    
}
