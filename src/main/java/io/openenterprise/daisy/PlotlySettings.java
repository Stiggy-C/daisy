package io.openenterprise.daisy;

import lombok.Data;
import plotly.Config;
import plotly.layout.Layout;

@Data
public class PlotlySettings extends PlotSettings {

    protected Boolean addSuffixIfExists = true;

    protected Config config;

    protected Layout layout;

    protected Boolean openInBrowser = false;

    protected Boolean useCdn = true;

}
