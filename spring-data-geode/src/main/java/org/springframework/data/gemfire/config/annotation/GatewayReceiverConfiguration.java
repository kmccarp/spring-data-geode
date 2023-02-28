/*
 * Copyright 2019-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.gemfire.config.annotation;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.geode.cache.wan.GatewayReceiver;

import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanReference;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.gemfire.config.annotation.support.AbstractAnnotationConfigSupport;
import org.springframework.data.gemfire.config.xml.GemfireConstants;
import org.springframework.data.gemfire.wan.GatewayReceiverFactoryBean;

/**
 * Spring {@link Configuration} class used to construct, configure and initialize a {@link GatewayReceiver} instance
 * in a Spring application context.
 *
 * @author Udo Kohlmeyer
 * @author John Blum
 * @see java.lang.annotation.Annotation
 * @see org.apache.geode.cache.wan.GatewayReceiver
 * @see org.springframework.beans.factory.support.BeanDefinitionBuilder
 * @see org.springframework.beans.factory.support.BeanDefinitionRegistry
 * @see org.springframework.context.annotation.Bean
 * @see org.springframework.context.annotation.Configuration
 * @see org.springframework.context.annotation.ImportBeanDefinitionRegistrar
 * @see org.springframework.core.annotation.AnnotationAttributes
 * @see org.springframework.core.type.AnnotationMetadata
 * @see org.springframework.data.gemfire.config.annotation.EnableGatewayReceiver
 * @see org.springframework.data.gemfire.config.annotation.support.AbstractAnnotationConfigSupport
 * @see org.springframework.data.gemfire.wan.GatewayReceiverFactoryBean
 * @since 2.2.0
 */
public class GatewayReceiverConfiguration extends AbstractAnnotationConfigSupport
		implements ImportBeanDefinitionRegistrar {

	static final boolean DEFAULT_MANUAL_START = GatewayReceiver.DEFAULT_MANUAL_START;

	static final int DEFAULT_START_PORT = GatewayReceiver.DEFAULT_START_PORT;
	static final int DEFAULT_END_PORT = GatewayReceiver.DEFAULT_END_PORT;
	static final int DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS = GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
	static final int DEFAULT_SOCKET_BUFFER_SIZE = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;

	static final String DEFAULT_BIND_ADDRESS = GatewayReceiver.DEFAULT_BIND_ADDRESS;
	static final String DEFAULT_HOSTNAME_FOR_SENDERS = GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS;

	private final String startPortLiteral = "startPort";
	private final String endPortLiteral = "endPort";
	private final String manualStartLiteral = "manualStart";
	private final String maximumTimeBetweenPingsLiteral = "maximumTimeBetweenPings";
	private final String socketBufferSizeLiteral = "socketBufferSize";
	private final String bindAddressLiteral = "bindAddress";
	private final String hostnameForSendersLiteral = "hostnameForSenders";
	private final String transportFiltersLiteral = "transportFilters";

	private final String startPortPropertyLiteral = "start-port";
	private final String endPortPropertyLiteral = "end-port";
	private final String manualStartPropertyLiteral = "manual-start";
	private final String maximumTimeBetweenPingsPropertyLiteral = "maximum-time-between-pings";
	private final String socketBufferSizePropertyLiteral = "socket-buffer-size";
	private final String bindAddressPropertyLiteral = "bind-address";
	private final String hostnameForSendersPropertyLiteral = "hostname-for-senders";
	private final String transportFiltersPropertyLiteral = "transport-filters";

	@Autowired(required = false)
	private List<GatewayReceiverConfigurer> gatewayReceiverConfigurers = Collections.emptyList();

	@Override
	protected Class<? extends Annotation> getAnnotationType() {
		return EnableGatewayReceiver.class;
	}

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

		if (isAnnotationPresent(importingClassMetadata)) {

			AnnotationAttributes enableGatewayReceiverAttributes = getAnnotationAttributes(importingClassMetadata);

			registerGatewayReceiverBeanDefinition(enableGatewayReceiverAttributes, registry);
		}
	}

	/**
	 * Configures a {@link GatewayReceiver} from the {@link EnableGatewayReceiver} annotation, <b><i>spring.data.gemfire.gateway.receiver.*</i></b>
	 * properties or {@link GatewayReceiverConfigurer}
	 *
	 * @param enableGatewayReceiverAttributes
	 * @param registry
	 */
	private void registerGatewayReceiverBeanDefinition(AnnotationAttributes enableGatewayReceiverAttributes,
			BeanDefinitionRegistry registry) {

		BeanDefinitionBuilder gatewayReceiverBeanBuilder =
			BeanDefinitionBuilder.genericBeanDefinition(GatewayReceiverFactoryBean.class);

		String gatewayReceiverBeanName = "GatewayReceiver";

		configureBeanFromAnnotationAttributes(enableGatewayReceiverAttributes, gatewayReceiverBeanBuilder,
			gatewayReceiverBeanName);

		configureBeanFromPropertiesOrWithDefaultValues(gatewayReceiverBeanBuilder);

		registerGatewayTransportFilterDependencies(enableGatewayReceiverAttributes, gatewayReceiverBeanBuilder);

		registry.registerBeanDefinition(gatewayReceiverBeanName, gatewayReceiverBeanBuilder.getBeanDefinition());
	}

	/**
	 * Configures GatewayReceiver {@link BeanDefinitionBuilder} using value populated on {@link EnableGatewayReceiver}.
	 * If no values are provided, values are set to defaults
	 *
	 * @param enableGatewayReceiverAttributes
	 * @param gatewayReceiverBeanBuilder
	 * @param gatewayReceiverBeanName
	 */
	private void configureBeanFromAnnotationAttributes(AnnotationAttributes enableGatewayReceiverAttributes,
			BeanDefinitionBuilder gatewayReceiverBeanBuilder, String gatewayReceiverBeanName) {

		gatewayReceiverBeanBuilder.addConstructorArgReference(GemfireConstants.DEFAULT_GEMFIRE_CACHE_NAME);

		gatewayReceiverBeanBuilder.addPropertyValue("beanName", gatewayReceiverBeanName);

		gatewayReceiverBeanBuilder.addPropertyReference("cache", GemfireConstants.DEFAULT_GEMFIRE_CACHE_NAME);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, startPortLiteral,
			enableGatewayReceiverAttributes.<Integer>getNumber(startPortLiteral), DEFAULT_START_PORT);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, endPortLiteral,
			enableGatewayReceiverAttributes.<Integer>getNumber(endPortLiteral), DEFAULT_END_PORT);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, manualStartLiteral,
			enableGatewayReceiverAttributes.getBoolean(manualStartLiteral), DEFAULT_MANUAL_START);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, maximumTimeBetweenPingsLiteral,
			enableGatewayReceiverAttributes.<Integer>getNumber(maximumTimeBetweenPingsLiteral),
			DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, socketBufferSizeLiteral,
			enableGatewayReceiverAttributes.<Integer>getNumber(socketBufferSizeLiteral), DEFAULT_SOCKET_BUFFER_SIZE);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, bindAddressLiteral,
			enableGatewayReceiverAttributes.getString(bindAddressLiteral), DEFAULT_BIND_ADDRESS);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, hostnameForSendersLiteral,
			enableGatewayReceiverAttributes.getString(hostnameForSendersLiteral), DEFAULT_HOSTNAME_FOR_SENDERS);

		setPropertyValueIfNotDefault(gatewayReceiverBeanBuilder, transportFiltersLiteral,
			resolveGatewayTransportFilterBeanReferences(
				enableGatewayReceiverAttributes.getStringArray(transportFiltersLiteral)),
			new ManagedList<>());
	}

	/**
	 * Configures GatewayReceiver {@link BeanDefinitionBuilder} using properties, defined under <b><i>spring.data.gemfire.gateway.receiver.*</i></b>
	 *
	 * @param gatewayReceiverBeanBuilder
	 */
	private void configureBeanFromPropertiesOrWithDefaultValues(BeanDefinitionBuilder gatewayReceiverBeanBuilder) {

		MutablePropertyValues beanPropertyValues =
			gatewayReceiverBeanBuilder.getRawBeanDefinition().getPropertyValues();

		gatewayReceiverBeanBuilder.addPropertyValue("gatewayReceiverConfigurers",
			resolveGatewayReceiverConfigurers());

		configureFromProperties(gatewayReceiverBeanBuilder, bindAddressLiteral, bindAddressPropertyLiteral,
			String.class, (String) beanPropertyValues.getPropertyValue(bindAddressLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder,
			hostnameForSendersLiteral, hostnameForSendersPropertyLiteral,
			String.class, (String) beanPropertyValues.getPropertyValue(hostnameForSendersLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder, manualStartLiteral, manualStartPropertyLiteral,
			Boolean.class, (Boolean) beanPropertyValues.getPropertyValue(manualStartLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder,
			maximumTimeBetweenPingsLiteral, maximumTimeBetweenPingsPropertyLiteral,
			Integer.class, (Integer) beanPropertyValues.getPropertyValue(maximumTimeBetweenPingsLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder, startPortLiteral, startPortPropertyLiteral,
			Integer.class, (Integer) beanPropertyValues.getPropertyValue(startPortLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder, endPortLiteral, endPortPropertyLiteral,
			Integer.class, (Integer) beanPropertyValues.getPropertyValue(endPortLiteral).getValue());

		configureFromProperties(gatewayReceiverBeanBuilder,
			socketBufferSizeLiteral, socketBufferSizePropertyLiteral,
			Integer.class, (Integer) beanPropertyValues.getPropertyValue(socketBufferSizeLiteral).getValue());

		String[] filters = resolveProperty(gatewayReceiverProperty(transportFiltersPropertyLiteral), String[].class);

		Optional.ofNullable(filters).ifPresent(transportFilters -> {

			ManagedList<BeanReference> beanReferences = resolveGatewayTransportFilterBeanReferences(transportFilters);

			gatewayReceiverBeanBuilder.addPropertyValue(transportFiltersLiteral, beanReferences);
		});
	}

	private <T> void configureFromProperties(BeanDefinitionBuilder gatewayReceiverBeanBuilder,
			String beanPropertyName, String propertyName, Class<T> propertyType, T annotationAttributeValue) {

		T propertyValue = resolveProperty(gatewayReceiverProperty(propertyName), propertyType,
			annotationAttributeValue);

		gatewayReceiverBeanBuilder.addPropertyValue(beanPropertyName, propertyValue);
	}

	private List<GatewayReceiverConfigurer> resolveGatewayReceiverConfigurers() {

		return Optional.ofNullable(this.gatewayReceiverConfigurers)
			.filter(gatewayReceiverConfigurers -> !gatewayReceiverConfigurers.isEmpty())
			.orElseGet(() ->
				Collections.singletonList(LazyResolvingComposableGatewayReceiverConfigurer.create(getBeanFactory())));
	}

	private ManagedList<BeanReference> resolveGatewayTransportFilterBeanReferences(
			String[] gatewayTransportFilterBeanNames) {

		ManagedList<BeanReference> gatewayTransportFilterBeanReferences = new ManagedList<>();

		Optional.ofNullable(gatewayTransportFilterBeanNames).ifPresent(it ->
			Arrays.stream(it)
				.map(RuntimeBeanReference::new)
				.forEach(gatewayTransportFilterBeanReferences::add));

		return gatewayTransportFilterBeanReferences;
	}

	private void registerGatewayTransportFilterDependencies(AnnotationAttributes annotationAttributes,
			BeanDefinitionBuilder gatewayReceiverBeanBuilder) {

		String[] transportFilters = annotationAttributes.getStringArray(transportFiltersLiteral);

		Optional.ofNullable(transportFilters).ifPresent(transportFilerBeanNames ->
			Arrays.stream(transportFilerBeanNames).forEach(gatewayReceiverBeanBuilder::addDependsOn));
	}

	private <T> BeanDefinitionBuilder setPropertyValueIfNotDefault(BeanDefinitionBuilder beanDefinitionBuilder,
			String propertyName, T value, T defaultValue) {

		return value != null
			? beanDefinitionBuilder.addPropertyValue(propertyName, value)
			: beanDefinitionBuilder.addPropertyValue(propertyName, defaultValue);
	}
}
