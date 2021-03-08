package io.ctsi.tenet.framework;


import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author Mc.D
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TenetMessage implements Serializable {
    private static final long serialVersionUID = 442217416049311842L;
    private String jsonData;
}
