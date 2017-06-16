package control;


import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

import static java.awt.Image.SCALE_DEFAULT;
import static java.awt.Image.SCALE_FAST;

class ImageProcessor {
    private static final int MAX_IMAGE_SAMPLE_HEIGHT=1024;
    private static final int MAX_IMAGE_SAMPLE_WIDTH=1024;

    private static Logger logger= LoggerFactory.getLogger(ImageProcessor.class);

    static BufferedImage  prepairImage(String base64){
        byte[] imageBytes = Base64.decodeBase64(base64);
        return prepairImage(imageBytes);
    }

    static BufferedImage  prepairImage(byte[] imageBytes){

        BufferedImage imgOut;
        try {
            // read image
            BufferedImage img = ImageIO.read(new ByteArrayInputStream(imageBytes));

            boolean scaleWidth=true;
            int w=img.getWidth();
            int h=img.getHeight();
            if(w==0 || h==0){
                logger.error("Image with zero width or height.");
                return null;
            }

            //缩放
            if(w<h){
                scaleWidth=false;
            }
            if(scaleWidth && w>MAX_IMAGE_SAMPLE_WIDTH){
                //缩放宽度
                double s=MAX_IMAGE_SAMPLE_WIDTH*1.0/w;
                Image newImg=img.getScaledInstance(MAX_IMAGE_SAMPLE_WIDTH,(int)(h*s),SCALE_FAST);
                img=toBufferedImage(newImg);
            }

            if(!scaleWidth && h>MAX_IMAGE_SAMPLE_HEIGHT){
                //缩放高度
                double s=MAX_IMAGE_SAMPLE_HEIGHT*1.0/h;
                Image newImg=img.getScaledInstance((int)(w*s),MAX_IMAGE_SAMPLE_HEIGHT,SCALE_FAST);
                img=toBufferedImage(newImg);
            }
            //减少透明块
            reduceTransparency(img);
            //返回值
            imgOut=img;
        }catch(Exception e){
            logger.error("Failed to process image.");
            e.printStackTrace();
            return null;
        }
        return imgOut;
    }

    private static BufferedImage toBufferedImage(Image img)
    {
        if (img instanceof BufferedImage)
        {
            return (BufferedImage) img;
        }
        // Create a buffered image with transparency
        BufferedImage bimage = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_ARGB);

        // Draw the image on to the buffered image
        Graphics2D bGr = bimage.createGraphics();
        bGr.drawImage(img, 0, 0, null);
        bGr.dispose();

        // Return the buffered image
        return bimage;
    }

    private static BufferedImage reduceTransparency(BufferedImage in){
        final int UNIFORM_IMAGE_WIDTH=160;
        final int UNIFORM_IMAGE_HEIGHT=160;
        final int UNIFORM_STEP=4;
        if(in.getTransparency()!=BufferedImage.TRANSLUCENT)
        {
            return in;
        }
        //SCALE
        BufferedImage uniformImage=toBufferedImage(in.getScaledInstance(UNIFORM_IMAGE_WIDTH,UNIFORM_IMAGE_HEIGHT,SCALE_DEFAULT));
        //EMPTY
        Graphics2D graph = uniformImage.createGraphics();

        int currentWriteX=0;
        int currentWriteY=0;
        boolean fullTranparent=true;
        for(int y=0;y<UNIFORM_IMAGE_HEIGHT;y+=UNIFORM_STEP){
            for(int x=0;x<UNIFORM_IMAGE_WIDTH;x+=UNIFORM_STEP){
                //read block
                BufferedImage sub=uniformImage.getSubimage(x,y,UNIFORM_STEP,UNIFORM_STEP);
                if(!totalTransparent(sub)){
                    fullTranparent=false;
                    graph.drawImage(sub,currentWriteX,currentWriteY,null);
                    if(currentWriteX<UNIFORM_IMAGE_WIDTH-UNIFORM_STEP){
                        currentWriteX+=UNIFORM_STEP;
                    }else{
                        currentWriteX=0;
                        currentWriteY+=UNIFORM_STEP;
                    }
                }
            }
        }

        graph.dispose();
        if(fullTranparent){
            logger.error("Whole image transparent.");
            return null;
        }

        int outputHeight=currentWriteY+UNIFORM_STEP;
        if(currentWriteX==0){
            outputHeight=currentWriteY;
        }
        return uniformImage.getSubimage(0,0,UNIFORM_IMAGE_WIDTH,outputHeight);

    }

    private static boolean totalTransparent(BufferedImage in){
        for(int y=0;y<in.getHeight();++y){
            for(int x=0;x<in.getWidth();++x){
                int t=(in.getRGB(x,y) & 0xFF000000 );
                if ( t != 0 ){
                    return false;
                }
            }
        }
        return true;
    }

//    static byte[] featureBytesFromImageBytes(byte[] imgBytes){
//        GlobalFeature feature=new CEDD();
//        byte[] featureBytes=null;
//        BufferedImage img;
//        try {
//            img = prepairImage(imgBytes);
//            if (img != null) {
//                feature.extract(img);
//                featureBytes = feature.getByteArrayRepresentation();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return featureBytes;
//    }

//    static String featureFromImage(String base64) {
//
//        String featureStr="";
//        byte[] featureBytes=featureBytesFromImage(base64);
//        if(null!=featureBytes){
//            featureStr = Base64.encodeBase64String(featureBytes);
//        }
//        return featureStr;
//    }

//    static byte[] featureBytesFromImage(String base64){
//        byte[] imageBytes = Base64.decodeBase64(base64);
//        return featureBytesFromImageBytes(imageBytes);
//    }
}
