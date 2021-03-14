package test.sean.dao;

import io.ctsi.tenet.app.TenetApplication;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {TenetApplication.class})
public class TestDemoDao {
    /*@Autowired
    private DemoDao demoDao;

    @Test
    public void getName(){
        DemoEntity demo = demoDao.queryDemoById(1);
        Assert.assertEquals("sean",demo.getName());
    }*/
}
