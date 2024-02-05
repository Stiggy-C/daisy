package io.openenterprise.daisy.spark

import io.openenterprise.daisy.DaisyApplication
import org.springframework.boot.SpringApplication

class DaisyApplication: DaisyApplication() {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication(DaisyApplication::class.java).apply { setAdditionalProfiles("spark") }.run(*args)
        }
    }
}