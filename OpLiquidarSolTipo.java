
package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import co.gov.ugpp.parafiscales.servicios.liquidador.contextotransaccionaltipo.v1.ContextoTransaccionalTipo;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import co.gov.ugpp.parafiscales.servicios.liquidador.identificaciontipo.v1.IdentificacionTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.expedientetipo.v1.ExpedienteTipo;


/**
 * <p>Clase Java para OpLiquidarSolTipo complex type.
 * 
 * <p>El siguiente fragmento de esquema especifica el contenido que se espera que haya en esta clase.
 * 
 * <pre>
 * &lt;complexType name="OpLiquidarSolTipo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="contextoTransaccional" type="{http://www.ugpp.gov.co/esb/schema/ContextoTransaccionalTipo/v1}ContextoTransaccionalTipo"/>
 *         &lt;element name="expediente" type="{http://www.ugpp.gov.co/schema/GestionDocumental/ExpedienteTipo/v1}ExpedienteTipo"/>
 *         &lt;element name="identificacion" type="{http://www.ugpp.gov.co/esb/schema/IdentificacionTipo/v1}IdentificacionTipo"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "OpLiquidarSolTipo", propOrder = {
    "contextoTransaccional",
    "expediente",
    "identificacion"
})
public class OpLiquidarSolTipo {

    @XmlElement(required = true)
    protected ContextoTransaccionalTipo contextoTransaccional;
    @XmlElement(required = true)
    protected ExpedienteTipo expediente;
    @XmlElement(required = true)
    protected IdentificacionTipo identificacion;

    /**
     * Obtiene el valor de la propiedad contextoTransaccional.
     * 
     * @return
     *     possible object is
     *     {@link ContextoTransaccionalTipo }
     *     
     */
    public ContextoTransaccionalTipo getContextoTransaccional() {
        return contextoTransaccional;
    }

    /**
     * Define el valor de la propiedad contextoTransaccional.
     * 
     * @param value
     *     allowed object is
     *     {@link ContextoTransaccionalTipo }
     *     
     */
    public void setContextoTransaccional(ContextoTransaccionalTipo value) {
        this.contextoTransaccional = value;
    }

    /**
     * Obtiene el valor de la propiedad expediente.
     * 
     * @return
     *     possible object is
     *     {@link ExpedienteTipo }
     *     
     */
    public ExpedienteTipo getExpediente() {
        return expediente;
    }

    /**
     * Define el valor de la propiedad expediente.
     * 
     * @param value
     *     allowed object is
     *     {@link ExpedienteTipo }
     *     
     */
    public void setExpediente(ExpedienteTipo value) {
        this.expediente = value;
    }

    /**
     * Obtiene el valor de la propiedad identificacion.
     * 
     * @return
     *     possible object is
     *     {@link IdentificacionTipo }
     *     
     */
    public IdentificacionTipo getIdentificacion() {
        return identificacion;
    }

    /**
     * Define el valor de la propiedad identificacion.
     * 
     * @param value
     *     allowed object is
     *     {@link IdentificacionTipo }
     *     
     */
    public void setIdentificacion(IdentificacionTipo value) {
        this.identificacion = value;
    }

}
