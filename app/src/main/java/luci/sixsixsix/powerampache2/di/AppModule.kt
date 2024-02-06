/**
 * Copyright (C) 2024  Antonio Tari
 *
 * This file is a part of Power Ampache 2
 * Ampache Android client application
 * @author Antonio Tari
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package luci.sixsixsix.powerampache2.di

import android.app.Application
import androidx.room.Room
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.components.SingletonComponent
import luci.sixsixsix.powerampache2.common.Constants.DB_LOCAL_NAME
import luci.sixsixsix.powerampache2.common.Constants.TIMEOUT_CONNECTION_S
import luci.sixsixsix.powerampache2.common.Constants.TIMEOUT_READ_S
import luci.sixsixsix.powerampache2.common.Constants.TIMEOUT_WRITE_S
import luci.sixsixsix.powerampache2.data.local.MusicDatabase
import luci.sixsixsix.powerampache2.data.mapping.AmpacheDateMapper
import luci.sixsixsix.powerampache2.data.remote.MainNetwork
import luci.sixsixsix.powerampache2.data.remote.MainNetwork.Companion.BASE_URL
import luci.sixsixsix.powerampache2.data.remote.PingScheduler
import luci.sixsixsix.powerampache2.domain.mappers.DateMapper
import luci.sixsixsix.powerampache2.domain.utils.AlarmScheduler
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit
import javax.inject.Singleton

@Module
@InstallIn(SingletonComponent::class)
object AppModule {
    @Provides
    @Singleton
    fun provideRetrofit(interceptor: Interceptor): Retrofit {
        val okHttpClient = OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .connectTimeout(TIMEOUT_CONNECTION_S, TimeUnit.SECONDS)
            .readTimeout(TIMEOUT_READ_S, TimeUnit.SECONDS)
            .writeTimeout(TIMEOUT_WRITE_S, TimeUnit.SECONDS)
            .addInterceptor(interceptor)
            .build()

        return Retrofit.Builder()
            .baseUrl(BASE_URL)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
    }

    @Provides
    fun provideDateMapper(): DateMapper =
        AmpacheDateMapper()

    @Provides
    @Singleton
    fun provideAmpacheApi(retrofit: Retrofit): MainNetwork =
        retrofit.create(MainNetwork::class.java)

    @Provides
    @Singleton
    fun provideWeakApplicationContext(application: Application) =
        WeakReference(application)

    @Provides
    @Singleton
    fun provideAlarmScheduler(application: Application): AlarmScheduler =
        PingScheduler(application)

    @Provides
    @Singleton
    fun provideMusicDatabase(application: Application): MusicDatabase =
        Room.databaseBuilder(
            application,
            MusicDatabase::class.java,
            DB_LOCAL_NAME
        )
        //.fallbackToDestructiveMigration()
        //.addMigrations(MIGRATION_73_74())
        .build()
}
